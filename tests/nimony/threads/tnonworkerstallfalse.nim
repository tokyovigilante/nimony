## The stall fallback must not fire while the pool is merely slow or busy.
##
## Each phase for each pool size (1 and 2) is its own child process, 10
## iterations each; main waits in `complete()` for about 400 ms, longer than
## `NonWorkerStallMs` (250 ms):
##   c  a long callee: the queues are empty and the callee spins for 400 ms on a
##      worker. Nothing is queued, so a drain would run nothing: the stall
##      counter stays 0 and no marker runs on main.
##   d  busy but progressing: the workers chew 2 ms tasks for 400 ms with the
##      stripes kept non-empty (the callee is re-queued behind them). Dequeues
##      keep the clock restarting: the stall counter stays 0 and no chew task
##      runs on main.
##   f   idle, then work arrives (the wait clock starts only on an observed
##       non-empty queue). Iteration i: a third thread enqueues a marker at
##       430 + 10 (i / 2) ms (even i) or 680 + 10 (i / 2) ms (odd i), and the
##       callee spins on a worker until 150 ms after that. The arrivals sit
##       just before ~501 ms and ~752 ms, where a clock that restarted
##       `accruing` by itself at each reset (instead of at the first observed
##       non-empty queue; its empty fires restart it at ~250, ~501 and ~752 ms)
##       would fire with the marker queued and run it on main. N = 2: the other
##       worker is idle and takes the marker at once. N = 1: the marker sits
##       behind the callee for ~150 ms (< 250 ms). Counter 0, no marker on main.
##       (An idle period of up to 720 ms must not count towards the 250 ms.)
##   g   (positive, kept here as the other half of the clock rules) submit-then-
##       wait loop, N = 1: the only worker is wedged on a task W that spins until
##       the task X queued behind it has run; main repeatedly submits a no-op
##       (it succeeds, the queues are not full) and then waits 10 ms on its own
##       lane. Neither the successful enqueue nor the own-lane completion may
##       stop the wait clock: X runs on main within 1 s via the fallback,
##       counter >= 1. W gives up after 2 s, so a pool without the fallback
##       fails instead of hanging.
## Worst case per child: c/d 10 x 0.45 s, f 10 x (arrival 430..720 + 150) ms = about 7.4 s
## (plus the 5 s marker wait only on failure), g 10 x 0.27 s;
## per file (f and g add three children) <= 40 s, far below the 120 s timeout.
## NW_TIMES=1 prints each child's wall time to stderr, NW_INFO=1 extra counters.

{.feature: "assumeSync".}  # test program: globals shared freely

import deps/nwlatch
import std / [atomics, threadpool, rawthreads, syncio, envvars, ioring]

type ChewFrame = object of CoroutineBase

const
  Iters = 10
  SpanMs = 400
  BehindMs = 150     # (f) how long the marker sits behind the callee (N = 1)
  GiveUpMs = 2000    # (g) W's own give-up

var
  deadline: int
  chewRuns: int
  chewOnMain: int
  chewFrame: ChewFrame

proc spinCallee() {.passive.} =
  let c = delay()
  submit(c)
  suspend()
  # Resumed from a stripe, i.e. on a worker while the pool is live.
  let end0 = nowMs() + SpanMs
  while nowMs() < end0: cpuRelax()

proc lingerCallee() {.passive.} =
  while nowMs() < deadline:
    let c = delay()
    submit(c)
    suspend()

proc chewFn(coro: ptr CoroutineBase): Continuation {.nimcall.} =
  if not isPoolWorker():
    discard atomicFetchAdd(chewOnMain, 1, moSequentiallyConsistent)
  discard atomicFetchAdd(chewRuns, 1, moRelaxed)
  let end0 = nowMs() + 2
  while nowMs() < end0: cpuRelax()
  if nowMs() < deadline:
    result = Continuation(fn: chewFn, env: coro)
  else:
    result = Continuation(fn: nil, env: nil)

type GFrame = object of CoroutineBase

var
  gFrame: GFrame
  gGate, gXRan, gWIn, gWDone: int
  arriveThread: RawThread
  arriveMs, longMs: int    # (f) this iteration's marker arrival and callee end

proc longCallee() {.passive.} =
  let c = delay()
  submit(c)
  suspend()
  let end0 = nowMs() + longMs
  while nowMs() < end0: cpuRelax()

proc arriveMain(arg: pointer) {.nimcall.} =
  sleepMs(arriveMs)
  submit(markerCont(), 0)

proc gWFn(env: ptr CoroutineBase): Continuation {.nimcall.} =
  atomicStore(gWIn, 1, moSequentiallyConsistent)
  let t0 = nowMs()
  while atomicLoad(gGate, moAcquire) == 0 and nowMs() - t0 < GiveUpMs: cpuRelax()
  atomicStore(gWDone, 1, moSequentiallyConsistent)
  result = Continuation(fn: nil, env: nil)

proc gXFn(env: ptr CoroutineBase): Continuation {.nimcall.} =
  atomicStore(gGate, 1, moRelease)
  atomicStore(gXRan, if isPoolWorker(): 1 else: 2, moRelease)
  result = Continuation(fn: nil, env: nil)

proc parkOnTimer(ms: int) {.passive.} =
  let c = delay()
  discard submitTimeout(afterMs(ms), c)
  suspend()

proc childG() =
  startWatchdog(55)
  var aborted, late, notOnMain, noFallback = 0
  var maxMs = 0
  for it in 0 ..< Iters:
    resetCounters()
    atomicStore(gGate, 0, moSequentiallyConsistent)
    atomicStore(gXRan, 0, moSequentiallyConsistent)
    atomicStore(gWIn, 0, moSequentiallyConsistent)
    atomicStore(gWDone, 0, moSequentiallyConsistent)
    submit(Continuation(fn: gWFn, env: addr gFrame), 0)
    if not waitFor(gWIn, 1, 2000):
      inc aborted
      continue
    submit(Continuation(fn: gXFn, env: addr gFrame), 0)    # queued behind W
    let before = stallRuns()
    let t0 = nowMs()
    while atomicLoad(gXRan, moAcquire) == 0 and nowMs() - t0 < GiveUpMs - 300:
      submit(noopCont(), 0)       # succeeds: the queues are far from full
      parkOnTimer(10)             # then an own-lane wait of 10 ms
    let dt = nowMs() - t0
    maxMs = max(maxMs, dt)
    if dt >= 1000: inc late
    if atomicLoad(gXRan, moAcquire) != 2: inc notOnMain
    if stallRuns() - before < 1: inc noFallback
    atomicStore(gGate, 1, moRelease)
    discard waitFor(gWDone, 1, 5000)
    discard quiesce()
  echo "N=", poolN, " phase=g iterations=", Iters, " aborted=", aborted,
    " late=", late, " x_not_on_main=", notOnMain, " nofallback=", noFallback
  if getEnv("NW_INFO").len > 0:
    echo "info N=", poolN, " phase=g max_ms=", maxMs, " stall_seam=", hasStallSeam,
      " stall_runs=", stallRuns()

proc child(phase: string) =
  if phase == "g":
    childG()
    return
  startWatchdog(55)
  var aborted = 0
  var stalled = 0
  var onmain = 0
  var maxMs = 0
  var minMs = 1000000
  for it in 0 ..< Iters:
    resetCounters()
    atomicStore(chewOnMain, 0, moSequentiallyConsistent)
    let before = stallRuns()
    let t0 = nowMs()
    if phase == "c":
      spinCallee()
    elif phase == "f":
      arriveMs = (if it mod 2 == 0: 430 else: 680) + 10 * (it div 2)
      longMs = arriveMs + BehindMs
      try:
        create arriveThread, arriveMain, nil
      except:
        echo "cannot start thread"
      longCallee()
      arriveThread.join()
      if not waitFor(markerRuns, 1, 5000): inc aborted
    else:
      deadline = t0 + SpanMs
      for k in 0 ..< 8:
        submit(Continuation(fn: chewFn, env: addr chewFrame), k)
      lingerCallee()
    let dt = nowMs() - t0
    maxMs = max(maxMs, dt)
    minMs = min(minMs, dt)
    if dt < (if phase == "f": longMs else: SpanMs) - 20: inc aborted      # the wait did not last as intended
    if stallRuns() != before: inc stalled
    if atomicLoad(markerOnMain, moSequentiallyConsistent) > 0 or
        atomicLoad(chewOnMain, moSequentiallyConsistent) > 0: inc onmain
    discard quiesce()
  echo "N=", poolN, " phase=", phase, " iterations=", Iters, " aborted=", aborted,
    " stalled=", stalled, " iterations_with_task_on_main=", onmain
  if getEnv("NW_INFO").len > 0:
    echo "info N=", poolN, " phase=", phase, " min_ms=", minMs, " max_ms=", maxMs,
      " stall_seam=", hasStallSeam, " stall_runs=", stallRuns()

when defined(windows):
  for ph in ["c", "d"]:
    for n in 1 .. 2:
      echo "N=", n, " phase=", ph, " iterations=10 aborted=0 stalled=0 iterations_with_task_on_main=0"
  for n in [2, 1]:
    echo "N=", n, " phase=f iterations=10 aborted=0 stalled=0 iterations_with_task_on_main=0"
  echo "N=1 phase=g iterations=10 aborted=0 late=0 x_not_on_main=0 nofallback=0"
else:
  if getEnv("NW_PHASE").len > 0:
    child(getEnv("NW_PHASE"))
  else:
    var envs: seq[string] = @[]
    for ph in ["c", "d"]:
      for n in 1 .. 2:
        envs.add "NW_POOL_N=" & $n & " NW_PHASE=" & ph & " timeout 60"
    envs.add "NW_POOL_N=2 NW_PHASE=f timeout 60"
    envs.add "NW_POOL_N=1 NW_PHASE=f timeout 60"
    envs.add "NW_POOL_N=1 NW_PHASE=g timeout 60"
    if not runChildren(envs):
      echo "FAIL"
