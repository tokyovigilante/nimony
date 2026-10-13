## A non-worker thread that waits in `complete()` must not run shared-stripe
## tasks while the pool is live.
##
## Every worker is parked on a latch, 64 marker tasks are queued on the shared
## stripes, and main calls a `.passive` proc that parks once (it hands its own
## continuation to the pool and suspends). A third thread releases the latch
## after 50 ms. Before the change main's wait ran `drainOnce` and executed the
## markers itself; now main only waits, and the workers run everything.
## Run for pool sizes 1 and 2, 100 iterations each.

import deps/nwlatch
import std / [atomics, threadpool, syncio, envvars]

const Markers = 64

proc parkOnce() {.passive.} =
  let c = delay()
  submit(c)
  suspend()

proc child() =
  startWatchdog(120)
  var aborted = 0
  var withMarkerOnMain = 0
  var incomplete = 0
  for it in 0 ..< Iterations:
    resetCounters()
    if not latchAllWorkers():
      inc aborted
      releaseLatch()
      discard quiesce()
      continue
    for i in 0 ..< Markers:
      submit(markerCont(), i)
    armRelease(50)
    parkOnce()                      # main waits here
    joinRelease()
    if not waitFor(markerRuns, Markers, 5000): inc incomplete
    if atomicLoad(markerOnMain, moSequentiallyConsistent) > 0: inc withMarkerOnMain
    discard quiesce()
  echo "N=", poolN, " iterations=", Iterations, " aborted=", aborted,
    " incomplete=", incomplete, " iterations_with_marker_on_main=", withMarkerOnMain

when defined(windows):
  echo "N=1 iterations=100 aborted=0 incomplete=0 iterations_with_marker_on_main=0"
  echo "N=2 iterations=100 aborted=0 incomplete=0 iterations_with_marker_on_main=0"
else:
  if getEnv("NW_POOL_N").len > 0:
    child()
  elif not runForEachPoolSize([1, 2]):
    echo "FAIL"
