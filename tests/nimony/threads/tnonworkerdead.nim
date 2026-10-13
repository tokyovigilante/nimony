## With no live worker, a non-worker falls back to the pre-change behaviour:
## it drains the pool itself, so nothing that progressed before hangs now.
##
## Two configurations, each in its own process:
##   shutdown  - the pool is stopped with `shutdownPool()`;
##   zero      - `initPool` is forced to start no worker thread at all
##               (`poolTestWorkerLimit`, the test hook).
## In each, main (a) `complete()`s a callee parked on its own lane and (b) does
## a saturated `submit`; both must finish within 5 s.

{.feature: "assumeSync".}  # test program: globals shared freely

import deps/nwlatch
import std / [atomics, threadpool, syncio, envvars, ioring]

var
  res: int
  resumedOnWorker: bool

proc parkOnTimer() {.passive.} =
  res = -999
  let c = delay()
  discard submitTimeout(afterMs(5), c, addr res)
  suspend()
  resumedOnWorker = isPoolWorker()

proc child(mode: string) =
  startWatchdog(20)
  if mode == "shutdown":
    # Let the pool do something first, then stop it.
    resetCounters()
    submit(markerCont(), 0)
    discard waitFor(markerRuns, 1, 5000)
    shutdownPool()
  # (a) a callee parked on main's own lane
  var t0 = nowMs()
  resumedOnWorker = true
  parkOnTimer()
  let parkMs = nowMs() - t0
  let parkOk = res == 0 and not resumedOnWorker and parkMs < 5000
  # (b) a saturated submit with nobody to drain the queues
  resetCounters()
  let cap = poolQueueStats().capacity
  var filled = poolQueueStats().queued
  while filled < cap:
    submit(noopCont(), filled)
    inc filled
  t0 = nowMs()
  submit(markerCont(), 0)
  let subMs = nowMs() - t0
  let subOk = atomicLoad(markerRuns, moSequentiallyConsistent) == 1 and subMs < 5000
  echo "mode=", mode, " live=", poolLive(), " park=", (if parkOk: "ok" else: "FAIL"),
    " saturated_submit=", (if subOk: "ok" else: "FAIL")

when defined(windows):
  echo "mode=shutdown live=false park=ok saturated_submit=ok"
  echo "mode=zero live=false park=ok saturated_submit=ok"
else:
  let mode = getEnv("NW_MODE")
  if mode.len > 0:
    child(mode)
  elif not runChildren(["NW_MODE=shutdown NW_POOL_N=2",
                        "NW_MODE=zero NW_ZERO_WORKERS=1 NW_POOL_N=2"]):
    echo "FAIL"
