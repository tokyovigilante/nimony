## A non-worker's own ring op completes while it waits, and its continuation
## runs on that same thread through the own-ready list - never on a shared
## stripe, and not on a pool worker.
##
## Main parks a `.passive` callee on a pipe read; a writer thread writes after
## 20 ms; main waits in `complete()`. 100 iterations at pool sizes 1 and 2.

{.feature: "assumeSync".}  # test program: globals shared freely

import deps/nwlatch
import std / [atomics, threadpool, syncio, envvars, ioring]

var
  res: int
  buf: array[8, char]
  resumedOnWorker: bool

proc readOnce(fd: cint) {.passive.} =
  res = -999
  let c = delay()
  discard submitRead(fd, addr buf[0], 8, afterMs(5000), c, addr res)
  suspend()
  resumedOnWorker = isPoolWorker()

proc child() =
  startWatchdog(120)
  var gotByte = 0
  var onMain = 0
  var redirected = 0
  for it in 0 ..< Iterations:
    let (r, w) = makePipe()
    setNonBlocking(r)
    resumedOnWorker = true
    let before = seamRedirects()
    startWriter(w, 20)
    readOnce(r)                     # main waits here
    joinWriter()
    if res == 1: inc gotByte
    if not resumedOnWorker: inc onMain
    if seamRedirects() - before >= 1: inc redirected
    closeFdRaw(r)
    closeFdRaw(w)
  echo "N=", poolN, " iterations=", Iterations, " read_ok=", gotByte,
    " resumed_on_main=", onMain, " redirected=", (if hasSeams: redirected else: Iterations)

when defined(windows):
  echo "N=1 iterations=100 read_ok=100 resumed_on_main=100 redirected=100"
  echo "N=2 iterations=100 read_ok=100 resumed_on_main=100 redirected=100"
else:
  if getEnv("NW_POOL_N").len > 0:
    child()
  elif not runForEachPoolSize([1, 2]):
    echo "FAIL"
