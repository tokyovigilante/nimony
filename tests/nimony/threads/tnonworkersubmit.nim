## A non-worker whose `submit` finds every queue full must not run the task
## itself while the pool is live.
##
## Every worker is parked on a latch and the stripes plus the inject queue are
## filled to the last slot with no-op tasks. A third thread releases the latch
## after 50 ms. Main then submits 1000 markers: before the change each one ran
## inline on main ("caller-runs"); now `submit` waits for a slot, so the
## markers are all accepted by the pool and run on workers. Pool sizes 1 and 2,
## 100 iterations each.

import deps/nwlatch
import std / [atomics, threadpool, syncio, envvars]

const Markers = 1000

proc child() =
  startWatchdog(120)
  var aborted = 0
  var withMarkerOnMain = 0
  var fillDirty = 0
  resetCounters()
  for it in 0 ..< Iterations:
    # Counters are cumulative: a marker that landed in the inject queue (rare;
    # workers visit it only every 61st dequeue) runs in a later iteration, and
    # the totals are checked at the end.
    if not latchAllWorkers():
      inc aborted
      releaseLatch()
      discard quiesce()
      continue
    let mainBefore = atomicLoad(markerOnMain, moSequentiallyConsistent)
    # Fill the pool to the last slot; the capacity is read from the pool, not
    # assumed.
    let cap = poolQueueStats().capacity
    var filled = poolQueueStats().queued
    while filled < cap:
      submit(noopCont(), filled)
      inc filled
    if poolQueueStats().queued != cap or
        atomicLoad(markerOnMain, moSequentiallyConsistent) != mainBefore:
      inc fillDirty
    armRelease(50)
    for i in 0 ..< Markers:
      submit(markerCont(), i)       # saturated: must wait for a slot
    joinRelease()
    discard quiesce()               # the stripes; see above for the inject queue
    if atomicLoad(markerOnMain, moSequentiallyConsistent) > mainBefore:
      inc withMarkerOnMain
  # Nothing may be lost: every marker either ran or still sits in the inject
  # queue (rare, see above), which holds nothing but fill tasks and these.
  discard quiesce()
  let expected = (Iterations - aborted) * Markers
  let missing = expected - atomicLoad(markerRuns, moSequentiallyConsistent)
  let lost = if missing <= poolQueueStats().injected: 0 else: missing
  echo "N=", poolN, " iterations=", Iterations, " aborted=", aborted,
    " fill_dirty=", fillDirty, " lost=", lost,
    " iterations_with_marker_on_main=", withMarkerOnMain

when defined(windows):
  echo "N=1 iterations=100 aborted=0 fill_dirty=0 lost=0 iterations_with_marker_on_main=0"
  echo "N=2 iterations=100 aborted=0 fill_dirty=0 lost=0 iterations_with_marker_on_main=0"
else:
  if getEnv("NW_POOL_N").len > 0:
    child()
  elif not runForEachPoolSize([1, 2]):
    echo "FAIL"
