## The own-ready redirect is gated to a live-pool non-worker `passiveWait`.
## Every other way main can drive its lane keeps submitting completions to the
## pool, and `inOwnWait` is clear once `passiveWait` returns.
##
## One process, phases in order, 100 iterations at pool sizes 1 and 2:
##   1  main complete()s a callee parked on its own lane: resumed on main via
##      the own-ready list (redirect seam counter moves);
##   2  main arms a timer op with a continuation and drives only `gReactor`
##      (what harness `pumpIo` does);
##   3  the same, driven by `pollCompletions`;
##   4  a continuation-bearing read is cancelled by `submitPollRemove` (4a) and
##      by `closeFd` (4b), main polling afterwards.
## Phases 2-4 assert: the continuation ran on a pool worker, the redirect
## counter did not move, and `inOwnWait` is false.

{.feature: "assumeSync".}  # test program: globals shared freely

import deps/nwlatch
import std / [atomics, threadpool, syncio, envvars, ioring]

var
  res: int
  resumedOnWorker: bool
  ranDone: int
  ranOnWorker: bool
  ranInOwnWait: bool

proc parkOnTimer() {.passive.} =
  res = -999
  let c = delay()
  discard submitTimeout(afterMs(2), c, addr res)
  suspend()
  resumedOnWorker = isPoolWorker()

proc recordRun() {.passive.} =
  ranOnWorker = isPoolWorker()
  ranInOwnWait = seamInOwnWait()
  atomicStore(ranDone, 1, moRelease)

proc arm() =
  atomicStore(ranDone, 0, moSequentiallyConsistent)
  ranOnWorker = false
  ranInOwnWait = true

proc awaitRun(drive: int): bool =
  ## Main drives its lane without ever calling `complete()`.
  var comps = default(array[16, IoCompletion])
  let t0 = nowMs()
  while atomicLoad(ranDone, moAcquire) == 0:
    if nowMs() - t0 > 3000: return false
    if drive == 0: discard gReactor(0)
    else: discard pollCompletions(comps)
    sleepMs(1)
  result = true

proc phaseOk(drive: int; before: int): bool =
  result = awaitRun(drive) and ranOnWorker and not ranInOwnWait and
           not seamInOwnWait() and seamRedirects() == before

proc child() =
  startWatchdog(120)
  var ok1, ok2, ok3, ok4a, ok4b = 0
  for it in 0 ..< Iterations:
    # 1
    resumedOnWorker = true
    var before = seamRedirects()
    parkOnTimer()
    if not resumedOnWorker and (not hasSeams or seamRedirects() - before >= 1) and
        not seamInOwnWait():
      inc ok1
    # 2
    arm()
    before = seamRedirects()
    discard submitTimeout(afterMs(2), delay(recordRun()), addr res)
    if phaseOk(0, before): inc ok2
    # 3
    arm()
    before = seamRedirects()
    discard submitTimeout(afterMs(2), delay(recordRun()), addr res)
    if phaseOk(1, before): inc ok3
    # 4a
    var (r, w) = makePipe()
    setNonBlocking(r)
    arm()
    before = seamRedirects()
    discard submitRead(r, addr res, 1, afterMs(5000), delay(recordRun()), addr res)
    submitPollRemove(r)
    if phaseOk(0, before): inc ok4a
    closeFdRaw(r)
    closeFdRaw(w)
    # 4b
    (r, w) = makePipe()
    setNonBlocking(r)
    arm()
    before = seamRedirects()
    discard submitRead(r, addr res, 1, afterMs(5000), delay(recordRun()), addr res)
    discard gReactor(0)                       # let the read reach its slot first
    closeFd(r)
    if phaseOk(0, before): inc ok4b
    closeFdRaw(w)
  echo "N=", poolN, " iterations=", Iterations, " p1=", ok1, " p2=", ok2,
    " p3=", ok3, " p4a=", ok4a, " p4b=", ok4b

when defined(windows):
  echo "N=1 iterations=100 p1=100 p2=100 p3=100 p4a=100 p4b=100"
  echo "N=2 iterations=100 p1=100 p2=100 p3=100 p4a=100 p4b=100"
else:
  if getEnv("NW_POOL_N").len > 0:
    child()
  elif not runForEachPoolSize([1, 2]):
    echo "FAIL"
