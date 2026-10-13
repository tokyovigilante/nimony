## Shared scaffolding for the tnonworker* tests: a deterministic worker latch,
## marker tasks that record which kind of thread ran them, a timed releaser, a
## watchdog and a way to run a test body once per pool size.
##
## IMPORT THIS BEFORE `std/ioring`. Importing the ring starts the pool with the
## default size, and `initPool` ignores any later count; this module's top
## level starts it first, with the size (`NW_POOL_N`, default 2) and the
## worker limit (`NW_ZERO_WORKERS=1` starts none) the parent test asked for.

{.feature: "assumeSync".}  # test scaffolding: globals shared freely

import std / [atomics, threadpool, rawthreads, envvars, syncio, cmdline, os, osproc, strutils]
when not defined(windows):
  import std/posix/posix

proc envInt(name: string; default: int): int =
  let v = getEnv(name)
  if v.len == 0: return default
  result = 0
  for ch in v:
    result = result * 10 + (ord(ch) - ord('0'))

let poolN* = envInt("NW_POOL_N", 2)
if getEnv("NW_ZERO_WORKERS") == "1":
  poolTestWorkerLimit = 0
initPool(poolN)

let Iterations* = envInt("NW_ITERS", 100)

# ------------------------------------------------------------------ time ---

proc sleepMs*(ms: int) =
  when defined(windows):
    discard
  else:
    var ts = Timespec(tv_sec: Time(ms div 1000), tv_nsec: clong((ms mod 1000) * 1_000_000))
    var rem = Timespec()
    discard nanosleep(ts, rem)

proc nowMs*(): int =
  when defined(windows):
    result = 0
  else:
    var ts = default(Timespec)
    discard clock_gettime(CLOCK_MONOTONIC, ts)
    result = int(ts.tv_sec) * 1000 + int(ts.tv_nsec) div 1_000_000

# -------------------------------------------------------------- watchdog ---

var wdSeconds: int
var wdThread: RawThread

proc watchdogMain(arg: pointer) {.nimcall.} =
  var waited = 0
  while waited < wdSeconds * 10:
    sleepMs(100)
    inc waited
  echo "WATCHDOG: timed out after ", wdSeconds, " s"
  stdout.flushFile()
  exitnow(3)

proc startWatchdog*(seconds: int) =
  ## Kills the process with status 3 if the test is still running after
  ## `seconds`: a hang is a failure, not a stuck suite.
  wdSeconds = seconds
  try:
    create wdThread, watchdogMain, nil
  except:
    echo "cannot start watchdog"

# ----------------------------------------------------------------- latch ---

type
  EmptyFrame = object of CoroutineBase

var
  frame: EmptyFrame
  latchReleased: int
  latchStarted: int
  latchRunning: int      # latch tasks inside latchFn right now
  latchIds: int          # bit mask of worker ids that entered a latch
  latchOnNonWorker: int  # latch tasks that ran on a non-worker (must stay 0)
  markerRuns*: int
  markerOnMain*: int     ## marker runs on a thread that is not a pool worker
  noopRuns*: int

proc latchFn(coro: ptr CoroutineBase): Continuation {.nimcall.} =
  if isPoolWorker():
    discard atomicFetchOr(latchIds, 1 shl threadIdx, moSequentiallyConsistent)
  else:
    discard atomicFetchAdd(latchOnNonWorker, 1, moSequentiallyConsistent)
  discard atomicFetchAdd(latchRunning, 1, moSequentiallyConsistent)
  discard atomicFetchAdd(latchStarted, 1, moSequentiallyConsistent)
  while atomicLoad(latchReleased, moAcquire) == 0:
    cpuRelax()
  discard atomicFetchSub(latchRunning, 1, moSequentiallyConsistent)
  result = Continuation(fn: nil, env: nil)

proc markerFn(coro: ptr CoroutineBase): Continuation {.nimcall.} =
  if not isPoolWorker():
    discard atomicFetchAdd(markerOnMain, 1, moSequentiallyConsistent)
  discard atomicFetchAdd(markerRuns, 1, moSequentiallyConsistent)
  result = Continuation(fn: nil, env: nil)

proc noopFn(coro: ptr CoroutineBase): Continuation {.nimcall.} =
  discard atomicFetchAdd(noopRuns, 1, moRelaxed)
  result = Continuation(fn: nil, env: nil)

proc markerCont*(): Continuation = Continuation(fn: markerFn, env: addr frame)
proc noopCont*(): Continuation = Continuation(fn: noopFn, env: addr frame)

proc waitFor*(location: var int; atLeast: int; timeoutMs: int): bool =
  let t0 = nowMs()
  while atomicLoad(location, moAcquire) < atLeast:
    if nowMs() - t0 > timeoutMs: return false
    sleepMs(0)
  result = true

proc resetCounters*() =
  atomicStore(markerRuns, 0, moSequentiallyConsistent)
  atomicStore(markerOnMain, 0, moSequentiallyConsistent)
  atomicStore(noopRuns, 0, moSequentiallyConsistent)

proc stripesEmpty*(): bool =
  ## The shared stripes hold nothing. The inject queue is deliberately not
  ## looked at: workers visit it only every 61st dequeue, so after a fill it
  ## drains far more slowly than the stripes do (existing behaviour).
  let st = poolQueueStats()
  result = st.queued - st.injected == 0

proc latchAllWorkers*(): bool =
  ## Park every worker on a latch task, deterministically: latch k (hint k) is
  ## submitted only after latch k-1 has recorded that it started and which
  ## worker it is on. Loading may start only when `poolN` latches run on
  ## `poolN` distinctIds workers and the stripes are empty.
  # The previous round's latches must have left latchFn before the flag re-arms.
  let t0 = nowMs()
  while atomicLoad(latchRunning, moSequentiallyConsistent) > 0:
    if nowMs() - t0 > 5000: return false
    sleepMs(0)
  atomicStore(latchReleased, 0, moSequentiallyConsistent)
  atomicStore(latchStarted, 0, moSequentiallyConsistent)
  atomicStore(latchIds, 0, moSequentiallyConsistent)
  atomicStore(latchOnNonWorker, 0, moSequentiallyConsistent)
  for k in 0 ..< poolN:
    submit(Continuation(fn: latchFn, env: addr frame), k)
    if not waitFor(latchStarted, k + 1, 2000): return false
  var distinctIds = 0
  let ids = atomicLoad(latchIds, moSequentiallyConsistent)
  for b in 0 ..< 62:
    if ((ids shr b) and 1) != 0: inc distinctIds
  result = atomicLoad(latchStarted, moSequentiallyConsistent) == poolN and distinctIds == poolN and
           atomicLoad(latchOnNonWorker, moSequentiallyConsistent) == 0 and
           stripesEmpty()

proc releaseLatch*() =
  atomicStore(latchReleased, 1, moRelease)

# -------------------------------------------------------------- releaser ---

var
  releaseAfterMs: int
  releaseThread: RawThread

proc releaserMain(arg: pointer) {.nimcall.} =
  sleepMs(releaseAfterMs)
  releaseLatch()

proc armRelease*(ms: int) =
  ## A third thread releases the latch after `ms`.
  releaseAfterMs = ms
  try:
    create releaseThread, releaserMain, nil
  except:
    echo "cannot start releaser"

proc joinRelease*() =
  releaseThread.join()

proc quiesce*(): bool =
  ## Wait until the shared stripes are empty.
  let t0 = nowMs()
  while not stripesEmpty():
    if nowMs() - t0 > 5000: return false
    sleepMs(1)
  result = true

# --------------------------------------------------------- child runner ---

proc runChildren*(envs: openArray[string]): bool =
  ## Parent mode: re-run this executable once per entry of `envs` (a shell
  ## environment prefix such as "NW_POOL_N=2") and print what the children
  ## print. A pool starts once per process, so each configuration needs its own.
  result = true
  for e in envs:
    try:
      let (outp, code) = execCmdEx(e & " " & getAppFilename())
      stdout.write outp
      if code != 0:
        echo "child [", e, "] failed with status ", code
        result = false
    except:
      echo "cannot run child"
      result = false

proc runForEachPoolSize*(sizes: openArray[int]): bool =
  var envs: seq[string] = @[]
  for n in sizes: envs.add "NW_POOL_N=" & $n
  result = runChildren(envs)

# ------------------------------------------------------------------ seams ---
# The redirect seams exist only on the changed pool; the shim lets the same
# test compile against the unchanged one (where it reports "no seams").

when compiles(inOwnWaitNow()):
  const hasSeams* = true
  proc seamInOwnWait*(): bool = inOwnWaitNow()
  proc seamRedirects*(): int = ownWaitRedirects()
else:
  const hasSeams* = false
  proc seamInOwnWait*(): bool = false
  proc seamRedirects*(): int = 0

# ------------------------------------------------------------------- pipe ---

when not defined(windows):
  proc cpipe(fds: ptr UncheckedArray[cint]): cint {.importc: "pipe".}

  proc makePipe*(): (cint, cint) =
    var fds = default(array[2, cint])
    if cpipe(cast[ptr UncheckedArray[cint]](addr fds)) != 0:
      quit "pipe failed"
    result = (fds[0], fds[1])

  var
    writerFd: cint
    writerDelayMs: int
    writerThread: RawThread

  proc writerMain(arg: pointer) {.nimcall.} =
    sleepMs(writerDelayMs)
    var b = 'x'
    discard write(writerFd, addr b, 1)

  proc startWriter*(fd: cint; delayMs: int) =
    ## A thread that writes one byte to `fd` after `delayMs`.
    writerFd = fd
    writerDelayMs = delayMs
    try:
      create writerThread, writerMain, nil
    except:
      echo "cannot start writer"

  proc joinWriter*() =
    writerThread.join()
