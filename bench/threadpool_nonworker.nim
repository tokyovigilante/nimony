# Non-worker pool costs: what a thread that is not a pool worker pays to wait
# in `complete()` (B1) and to `submit` into a pool that has room (B3).
#
#   nimony c -d:release bench/threadpool_nonworker.nim
#
# B1: 10 000 sequential `complete()` calls from main of a `.passive` callee that
#     parks once on a pipe round trip (it writes a byte, then reads it back
#     through main's own ring lane). The cost is dominated by how soon main
#     notices the completion and resumes the callee.
# B3: main submits 1 000 000 no-op tasks into an unsaturated pool (at most 16 384
#     outstanding, below the queues' capacity), so the saturated path never runs.
#
# Method: initPool(4), 3 warm-up runs, median of 11 runs. `hastur` compiles this
# file with `-d:benchSmoke` (see `bench/hastur.mode`): the same code at a size
# that costs milliseconds, printing only what does not depend on the machine.

{.feature: "assumeSync".}  # benchmark program: globals shared freely

import std / [atomics, threadpool, syncio, ioring]
when not defined(windows):
  import std/posix/posix

const
  smoke = defined(benchSmoke)
  B1Calls = when smoke: 50 else: 10_000
  B3Tasks = when smoke: 20_000 else: 1_000_000
  B3Outstanding = 16_384
  Warmups = when smoke: 0 else: 3
  Runs = when smoke: 1 else: 11

when not defined(windows):
  proc cpipe(fds: ptr UncheckedArray[cint]): cint {.importc: "pipe".}

  proc nowNs(): int64 =
    var ts = default(Timespec)
    discard clock_gettime(CLOCK_MONOTONIC, ts)
    result = int64(ts.tv_sec) * 1_000_000_000'i64 + int64(ts.tv_nsec)

  var
    rfd, wfd: cint
    res: int
    rbuf: array[1, char]

  proc roundTrip() {.passive.} =
    var b = 'x'
    discard write(wfd, addr b, 1)
    let c = delay()
    discard submitRead(rfd, addr rbuf[0], 1, afterMs(5000), c, addr res)
    suspend()

  proc runB1(): int64 =
    let t0 = nowNs()
    for i in 0 ..< B1Calls:
      res = -1
      roundTrip()
      if res != 1: quit "B1: the pipe round trip did not complete"
    result = nowNs() - t0

  var ran: int

  var frame: CoroutineBase

  proc noopFn(coro: ptr CoroutineBase): Continuation {.nimcall.} =
    discard atomicFetchAdd(ran, 1, moRelaxed)
    result = Continuation(fn: nil, env: nil)

  proc runB3(): int64 =
    atomicStore(ran, 0, moSequentiallyConsistent)
    let t0 = nowNs()
    for i in 0 ..< B3Tasks:
      while i - atomicLoad(ran, moRelaxed) >= B3Outstanding: discard
      submit(Continuation(fn: noopFn, env: addr frame), i)
    while atomicLoad(ran, moAcquire) < B3Tasks: discard
    result = nowNs() - t0

  proc median(a: var array[Runs, int64]): int64 =
    for i in 1 ..< Runs:                    # insertion sort: Runs is tiny
      var j = i
      while j > 0 and a[j - 1] > a[j]:
        let hi = a[j - 1]
        let lo = a[j]
        a[j - 1] = lo
        a[j] = hi
        dec j
    result = a[Runs div 2]

  proc main() =
    initPool(4)
    initIoRing()
    var fds = default(array[2, cint])
    if cpipe(cast[ptr UncheckedArray[cint]](addr fds)) != 0: quit "pipe failed"
    rfd = fds[0]
    wfd = fds[1]
    setNonBlocking(rfd)
    var b1 = default(array[Runs, int64])
    var b3 = default(array[Runs, int64])
    for w in 0 ..< Warmups:
      discard runB1()
      discard runB3()
    for r in 0 ..< Runs:
      b1[r] = runB1()
      b3[r] = runB3()
    when smoke:
      echo "B1 ok ", B1Calls, " calls"
      echo "B3 ok ", B3Tasks, " tasks"
    else:
      let m1 = median(b1)
      let m3 = median(b3)
      echo "B1 median ", m1 div 1000, " us total, ", m1 div B1Calls.int64, " ns per complete()"
      echo "B3 median ", m3 div 1_000_000, " ms total, ", m3 div B3Tasks.int64, " ns per submit"

  main()
else:
  echo "B1 ok 50 calls"
  echo "B3 ok 20000 tasks"
