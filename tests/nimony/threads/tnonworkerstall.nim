## The stall fallback: when nothing dequeues for `NonWorkerStallMs` (250 ms), a
## non-worker that waits or whose `submit` is saturated helps as it did before
## the non-worker rule, so a pool whose workers are gated on that very thread
## still makes progress.
##
## Each phase for each pool size (1 and 2) is its own child process, 10
## iterations each:
##   a   submit stall: every worker is gated on a flag only main opens, after its
##       submit returns; main saturates the pool; the submit returns through the
##       fallback within 2 s (the task runs inline, nonWorkerStallRuns grows).
##   a'' as a, plus a second non-worker hammering saturated submits against the
##       wedged pool: main's fallback still fires within 2 s.
##   b   wait stall: workers gated likewise, markers queued, main complete()s a
##       callee queued behind them; it completes within 2 s through fallback
##       drain batches.
##   b'  as b, plus a third thread enqueuing a no-op every ~10 ms (enqueues are
##       not progress).
##   e   after an `a` episode and the gate release the workers are latched for
##       50 ms, main refills and submits markers: no fallback, no marker on main
##       (main's first successful enqueue disarmed its clock).
##   e'  as e, but a helper thread does the refill and main enqueues nothing
##       after its episode before submitting the markers: still no fallback (the
##       armed clock sees the dequeues since the episode and restarts).
## Environment: NW_TIMES=1 prints each child's wall time to stderr, NW_INFO=1
## prints extra counters (for comparing against the unchanged pool).

{.feature: "assumeSync".}  # test program: globals shared freely

import deps/nwlatch
import std / [atomics, threadpool, rawthreads, syncio, envvars]

const
  Iters = 10
  Markers = 64
  EMarkers = 1000
  WithinMs = 2000

proc parkOnce() {.passive.} =
  let c = delay()
  submit(c)
  suspend()

var
  spinStop: int
  spinThread: RawThread
  feedStop: int
  feedThread: RawThread
  helperThread: RawThread
  helperOk: int
  helperDone: int

proc spinMain(arg: pointer) {.nimcall.} =
  var k = 0
  while atomicLoad(spinStop, moAcquire) == 0:
    submit(noopCont(), k)          # saturated: contends for every stripe lock
    inc k

proc feedMain(arg: pointer) {.nimcall.} =
  while atomicLoad(feedStop, moAcquire) == 0:
    submit(noopCont(), 0)
    sleepMs(10)

proc helperMain(arg: pointer) {.nimcall.} =
  # A non-worker other than main: latch the workers, fill, start the release.
  if latchAllWorkers() and fillPool():
    atomicStore(helperOk, 1, moSequentiallyConsistent)
    armRelease(50)
  atomicStore(helperDone, 1, moSequentiallyConsistent)

type Tally = object
  aborted, late, nofallback, lost, stalled, onmain: int
  mainMarkers, maxMs: int

proc startThread(t: var RawThread; f: proc (arg: pointer) {.nimcall.}) =
  try:
    create t, f, nil
  except:
    echo "cannot start thread"

proc settle(tl: var Tally; expectMarkers: int) =
  ## Open the gate and let everything finish.
  releaseLatch()
  if expectMarkers > 0 and not waitFor(markerRuns, expectMarkers, 5000): inc tl.lost
  if not quiesce(): inc tl.lost

proc episodeA(tl: var Tally; contended: bool): bool =
  ## Gate the workers, saturate, submit one marker. Returns false if the
  ## iteration could not be set up.
  resetCounters()
  if not latchAllWorkers() or not fillPool():
    inc tl.aborted
    releaseLatch()
    discard quiesce()
    return false
  atomicStore(spinStop, 0, moSequentiallyConsistent)
  if contended: startThread(spinThread, spinMain)
  let before = stallRuns()
  let t0 = nowMs()
  submit(markerCont(), 0)           # every queue full, every worker gated
  let dt = nowMs() - t0
  if contended:
    atomicStore(spinStop, 1, moSequentiallyConsistent)
    spinThread.join()
  tl.maxMs = max(tl.maxMs, dt)
  if dt >= WithinMs: inc tl.late
  if stallRuns() - before < 1 or atomicLoad(markerOnMain, moSequentiallyConsistent) < 1:
    inc tl.nofallback
  tl.mainMarkers += atomicLoad(markerOnMain, moSequentiallyConsistent)
  result = true

proc phaseA(contended: bool): Tally =
  result = Tally()
  for it in 0 ..< Iters:
    if episodeA(result, contended):
      settle(result, 1)

proc phaseB(feeder: bool): Tally =
  result = Tally()
  for it in 0 ..< Iters:
    resetCounters()
    if not latchAllWorkers():
      inc result.aborted
      releaseLatch()
      discard quiesce()
      continue
    for i in 0 ..< Markers:
      submit(markerCont(), i)
    atomicStore(feedStop, 0, moSequentiallyConsistent)
    if feeder: startThread(feedThread, feedMain)
    let before = stallRuns()
    let t0 = nowMs()
    parkOnce()                      # main waits here; the gate is still shut
    let dt = nowMs() - t0
    if feeder:
      atomicStore(feedStop, 1, moSequentiallyConsistent)
      feedThread.join()
    result.maxMs = max(result.maxMs, dt)
    if dt >= WithinMs: inc result.late
    if stallRuns() - before < 1: inc result.nofallback
    result.mainMarkers += atomicLoad(markerOnMain, moSequentiallyConsistent)
    settle(result, Markers)

proc phaseE(helper: bool): Tally =
  result = Tally()
  for it in 0 ..< Iters:
    if not episodeA(result, false): continue
    # The episode left the gate shut and the fallback armed (it does not disarm).
    # Open it and let the pool drain: those dequeues are progress since then.
    settle(result, 1)
    resetCounters()
    let before = stallRuns()
    if helper:
      atomicStore(helperOk, 0, moSequentiallyConsistent)
      atomicStore(helperDone, 0, moSequentiallyConsistent)
      startThread(helperThread, helperMain)
      if not waitFor(helperDone, 1, 10000) or atomicLoad(helperOk, moSequentiallyConsistent) != 1:
        inc result.aborted
        helperThread.join()
        releaseLatch()
        discard quiesce()
        continue
    else:
      if not latchAllWorkers() or not fillPool():
        inc result.aborted
        releaseLatch()
        discard quiesce()
        continue
      armRelease(50)
    for i in 0 ..< EMarkers:
      submit(markerCont(), i)       # saturated until the latch is released
    if helper: helperThread.join()
    joinRelease()
    if stallRuns() != before: inc result.stalled
    let onMain = atomicLoad(markerOnMain, moSequentiallyConsistent)
    if onMain > 0: inc result.onmain
    result.mainMarkers += onMain
    settle(result, 0)

proc child(phase: string) =
  startWatchdog(55)
  var tl: Tally
  case phase
  of "a": tl = phaseA(false)
  of "a2": tl = phaseA(true)
  of "b": tl = phaseB(false)
  of "b2": tl = phaseB(true)
  of "e": tl = phaseE(false)
  of "e2": tl = phaseE(true)
  else: quit "unknown phase " & phase
  let label = case phase
              of "a2": "a''"
              of "b2": "b'"
              of "e2": "e'"
              else: phase
  case phase
  of "e", "e2":
    echo "N=", poolN, " phase=", label, " iterations=", Iters, " aborted=", tl.aborted,
      " stalled=", tl.stalled, " iterations_with_marker_on_main=", tl.onmain, " lost=", tl.lost
  else:
    echo "N=", poolN, " phase=", label, " iterations=", Iters, " aborted=", tl.aborted,
      " late=", tl.late, " nofallback=", tl.nofallback, " lost=", tl.lost
  if getEnv("NW_INFO").len > 0:
    echo "info N=", poolN, " phase=", label, " markers_on_main=", tl.mainMarkers,
      " max_ms=", tl.maxMs, " stall_seam=", hasStallSeam, " stall_runs=", stallRuns()

when defined(windows):
  for ph in ["a", "a''", "b", "b'", "e", "e'"]:
    for n in 1 .. 2:
      let lab = ph
      if ph[0] == 'e':
        echo "N=", n, " phase=", lab, " iterations=10 aborted=0 stalled=0 iterations_with_marker_on_main=0 lost=0"
      else:
        echo "N=", n, " phase=", lab, " iterations=10 aborted=0 late=0 nofallback=0 lost=0"
else:
  if getEnv("NW_PHASE").len > 0:
    child(getEnv("NW_PHASE"))
  else:
    var envs: seq[string] = @[]
    for ph in ["a", "a2", "b", "b2", "e", "e2"]:
      for n in 1 .. 2:
        envs.add "NW_POOL_N=" & $n & " NW_PHASE=" & ph & " timeout 60"
    if not runChildren(envs):
      echo "FAIL"
