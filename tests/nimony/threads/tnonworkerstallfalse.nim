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
## NW_TIMES=1 prints each child's wall time to stderr, NW_INFO=1 extra counters.

{.feature: "assumeSync".}  # test program: globals shared freely

import deps/nwlatch
import std / [atomics, threadpool, syncio, envvars]

type ChewFrame = object of CoroutineBase

const
  Iters = 10
  SpanMs = 400

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

proc child(phase: string) =
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
    else:
      deadline = t0 + SpanMs
      for k in 0 ..< 8:
        submit(Continuation(fn: chewFn, env: addr chewFrame), k)
      lingerCallee()
    let dt = nowMs() - t0
    maxMs = max(maxMs, dt)
    minMs = min(minMs, dt)
    if dt < SpanMs - 20: inc aborted      # the wait did not last as intended
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
else:
  if getEnv("NW_PHASE").len > 0:
    child(getEnv("NW_PHASE"))
  else:
    var envs: seq[string] = @[]
    for ph in ["c", "d"]:
      for n in 1 .. 2:
        envs.add "NW_POOL_N=" & $n & " NW_PHASE=" & ph & " timeout 60"
    if not runChildren(envs):
      echo "FAIL"
