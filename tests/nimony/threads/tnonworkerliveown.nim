## A non-worker whose waits each end through its OWN lane still reaches the
## stall fallback. (The wait clock must not be stopped by own-lane work, by the
## back-off resetting, or by gaps between waits.)
##
## Pool of ONE worker. The worker runs W, which spins until task X has run; X
## was queued behind it (same stripe), so the pool is wedged: work queued,
## nothing dequeued. Main loops on short own-lane waits (a `.passive` callee
## parked on a timer, resumed through the own-ready list) until X has run:
##   10 ms    each wait ends after 10 ms,
##   1 ms     each wait ends after 1 ms,
##   100 us   a 100 us timer per wait.
## Every wait ends with own-lane work, so the back-off counter resets all the
## time. At 2593bf85 main's first wait drains X. With the stall fallback X runs
## on main after about 250 ms (`nonWorkerStallRuns` >= 1). It must be within 1 s.
## W gives up after 2 s on its own, so a pool without the fallback fails the
## test instead of hanging it.
##
## Each wait length is its own child process, 10 iterations each. Worst case on
## the fixed code per iteration: 250 ms + 1 ms + one wait + ~5 ms refill, i.e.
## ~0.27 s; per child ~2.7 s; per file ~8 s. Without the fallback an iteration
## is bounded by W's 2 s give-up: ~20 s per child. Child timeout 60 s, file
## timeout 120 s. NW_TIMES=1 prints each child's wall time to stderr.

{.feature: "assumeSync".}  # test program: globals shared freely

import deps/nwlatch
import std / [atomics, threadpool, syncio, envvars, ioring]

type LiveFrame = object of CoroutineBase

const
  Iters = 10
  WithinMs = 1000
  GiveUpMs = 2000

var
  gate, xRan, wIn, wDone: int
  frame2: LiveFrame

proc wFn(env: ptr CoroutineBase): Continuation {.nimcall.} =
  atomicStore(wIn, 1, moSequentiallyConsistent)
  let t0 = nowMs()
  while atomicLoad(gate, moAcquire) == 0 and nowMs() - t0 < GiveUpMs: cpuRelax()
  atomicStore(wDone, 1, moSequentiallyConsistent)
  result = Continuation(fn: nil, env: nil)

proc xFn(env: ptr CoroutineBase): Continuation {.nimcall.} =
  atomicStore(gate, 1, moRelease)
  atomicStore(xRan, if isPoolWorker(): 1 else: 2, moRelease)
  result = Continuation(fn: nil, env: nil)

proc parkOnTimer(us: int) {.passive.} =
  let c = delay()
  discard submitTimeout(after(int64(us) * 1000), c)
  suspend()

proc child(us: int) =
  startWatchdog(55)
  var aborted, late, notOnMain, noFallback, waits = 0
  var maxMs = 0
  for it in 0 ..< Iters:
    resetCounters()
    atomicStore(gate, 0, moSequentiallyConsistent)
    atomicStore(xRan, 0, moSequentiallyConsistent)
    atomicStore(wIn, 0, moSequentiallyConsistent)
    atomicStore(wDone, 0, moSequentiallyConsistent)
    submit(Continuation(fn: wFn, env: addr frame2), 0)
    if not waitFor(wIn, 1, 2000):
      inc aborted
      continue
    submit(Continuation(fn: xFn, env: addr frame2), 0)   # queued behind W
    let before = stallRuns()
    let t0 = nowMs()
    while atomicLoad(xRan, moAcquire) == 0 and nowMs() - t0 < GiveUpMs - 300:
      parkOnTimer(us)
      inc waits
    let dt = nowMs() - t0
    maxMs = max(maxMs, dt)
    if dt >= WithinMs: inc late
    if atomicLoad(xRan, moAcquire) != 2: inc notOnMain
    if hasStallSeam and stallRuns() - before < 1: inc noFallback
    atomicStore(gate, 1, moRelease)
    discard waitFor(wDone, 1, 5000)
    discard quiesce()
  echo "N=", poolN, " wait_us=", us, " iterations=", Iters, " aborted=", aborted,
    " late=", late, " x_not_on_main=", notOnMain, " nofallback=", noFallback
  if getEnv("NW_INFO").len > 0:
    echo "info wait_us=", us, " max_ms=", maxMs, " waits=", waits,
      " stall_seam=", hasStallSeam, " stall_runs=", stallRuns()

when defined(windows):
  for us in [10000, 1000, 100]:
    echo "N=1 wait_us=", us, " iterations=10 aborted=0 late=0 x_not_on_main=0 nofallback=0"
else:
  if getEnv("NW_WAIT_US").len > 0:
    var us = 0
    for ch in getEnv("NW_WAIT_US"): us = us * 10 + (ord(ch) - ord('0'))
    child(us)
  else:
    var envs: seq[string] = @[]
    for us in ["10000", "1000", "100"]:
      envs.add "NW_POOL_N=1 NW_WAIT_US=" & us & " timeout 60"
    if not runChildren(envs):
      echo "FAIL"
