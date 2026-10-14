# (c) 2025 Andreas Rumpf
# Lock-striped thread pool with continuation-based scheduling.
#
# Workers contend on independent stripes to reduce lock pressure.
# Each worker polls I/O every iteration; the timeout doubles as idle sleep.
#
# A Task wraps a Continuation plus metadata. The pool schedules Tasks;
# the worker trampolines the inner continuation.

# The pool is a synchronization primitive itself: its globals are guarded by
# its own stripe locks and atomics, which the shared-global check cannot see.
{.feature: "assumeSync".}

import std / [atomics, rawthreads, assertions, ticketlocks, private/syslocks, cpuinfo]

when not defined(windows):
  import std/posix/posix
else:
  import windows/winlean
# --- Configuration ---

import std/stripes

const
  StripeSize*  = 2048  ## Tasks per stripe; must be a power of 2.
  BulkSize*    = 32   ## Max tasks drained per bulk dequeue.
  StageSize*   = 256  ## Tasks held in a worker's private staging ring; must be a power of 2.

# --- Task = Continuation + metadata ---

type
  Task* = object
    ## A schedulable unit of work. Wraps a CPS Continuation so that
    ## workers can trampoline `.passive` procs. Extra fields can be
    ## added here for priority, cancellation tokens, diagnostics, etc.
    con*: Continuation

proc toTask*(c: Continuation): Task {.inline.} =
  Task(con: c)

# --- Task queue ---

proc poolPollIo*(timeoutMs: int): bool {.nimcall.} =
  ## Polls one I/O backend once; returns true if it delivered anything.
  ## `std/ioring` registers one of these.
  result = false

type
  WorkerMetrics* {.align: 64.} = object
    enqueueMiss*: uint
    dequeueMiss*: uint
    tasksSubmited*: uint
    tasksHandled*: uint
    tasksStealed*: uint

var
  workerMetrics*: seq[WorkerMetrics]
  localQueues: seq[FifoStripe[Task]]
  injectQueue: FifoStripe[Task]
  workers: seq[RawThread]
  stopFlag: bool # accessed atomically
  workerCount* = 0
  gReactor*: proc(timeoutMs: int): bool {.nimcall.} = poolPollIo
var gReactorState*: int = 0
  ## 0 until `gReactor`/`gReactorWaits` — and everything they reach — are fully
  ## set up. It has to be PUBLISHED rather than merely assigned: `initPool`
  ## starts the workers, and only then does `ioring.setupRing` allocate the
  ## per-lane arenas the reactor indexes and finally point `gReactor` at itself.
  ## Nothing orders those plain stores against each other, so on a weakly
  ## ordered machine a worker could see the new reactor and an arena seq that is
  ## still empty. `reactorReady` pairs an acquire with `setupRing`'s release.
var gReactorWaits* = false
  ## Does `gReactor(ms)` actually sleep for up to `ms`? Set from the I/O
  ## backend (see `BackendRelays.waits`). While it is false an idle worker has
  ## to do the sleeping itself, because a reactor that only peeks would
  ## otherwise leave the loop spinning on a core.
var threadIdx* {.threadvar.}: int
var isWorker {.threadvar.}: bool
  # Only workers may stage; foreign threads must enqueue directly or their
  # batch could never be flushed.

var
  liveWorkers: int
    ## Atomic. Worker threads that were actually started and have not exited.
    ## `initPool` swallows thread-creation failures, so `workerCount` alone does
    ## not say whether anyone is draining the stripes. A non-worker only stops
    ## running pool tasks itself (see `poolLive`) while this is positive.
  poolTestWorkerLimit*: int = -1
    ## Test hook: when non-negative, `initPool` starts at most this many worker
    ## threads (0 simulates every thread creation failing). Set it before the
    ## first `initPool`.

proc poolLive*(): bool {.inline.} =
  ## True while at least one worker is running and the pool has not been shut
  ## down. Non-worker threads leave the shared stripes to the workers only while
  ## this holds; otherwise nobody else would run the tasks (see `submit`,
  ## `passiveWait`).
  not atomicLoad(stopFlag, moRelaxed) and atomicLoad(liveWorkers, moRelaxed) > 0

# --- Back-off helpers for non-worker threads ---

when not defined(windows):
  proc sched_yield(): cint {.importc: "sched_yield", sideEffect.}

proc yieldThread() =
  when defined(windows):
    sleep(0'u32)
  else:
    discard sched_yield()

proc napMicros(us: int) =
  when defined(windows):
    sleep(1'u32)
  else:
    var ts = Timespec(tv_sec: Time(us div 1_000_000),
                      tv_nsec: clong((us mod 1_000_000) * 1000))
    var rem = Timespec()
    discard nanosleep(ts, rem)

# --- Stall detection for non-worker threads ---
#
# A non-worker leaves the shared stripes to the workers (see `submit`,
# `nonWorkerWait`) unless the pool looks wedged: no shared queue has removed a
# single task for `NonWorkerStallMs`. Then it helps exactly as it did before the
# change, so a program that progressed then still progresses (an extra delay of
# at most `NonWorkerStallMs` per stall episode, not per task).

const
  NonWorkerStallMs* {.intdefine.} = 250
    ## How long no queue may dequeue before a non-worker runs pool tasks itself.

var
  nonWorkerStallRuns*: int
    ## Atomic. How often a non-worker fell back to running pool work because
    ## the pool was stalled: saturated submits that ran their task inline plus
    ## waits that ran at least one task. Meant for tests and diagnostics.

type
  StallClock = object
    ## One per non-worker code path (`submit`, `passiveWait`), thread-local.
    armed: bool
    stallStart: int   ## ms; when the current quiet period began
    lastSum: int      ## `progressSum()` at `stallStart` or the last change
    lastSample: int   ## ms; the previous `passiveWait` sample

var
  submitClock {.threadvar.}: StallClock
  waitClock {.threadvar.}: StallClock
  inStallDrain {.threadvar.}: bool
    ## True while this non-worker runs a fallback batch in `nonWorkerWait`.

proc noteEnqueued() {.inline.} =
  ## A non-worker's successful enqueue disarms its submit clock. It also ends the
  ## wait clock's evidence: a thread that enqueues a task (typically the
  ## continuation of a passive call it is about to wait for) starts a new wait,
  ## and a clock left armed by an earlier wait would otherwise call the pool
  ## stalled although the task was queued a moment ago. The re-submits of the
  ## wait's own fallback batch do not count.
  if submitClock.armed: submitClock.armed = false
  if waitClock.armed and not inStallDrain: waitClock.armed = false

proc monoMs(): int =
  when defined(windows):
    proc GetTickCount64(): uint64 {.importc: "GetTickCount64", stdcall, dynlib: "kernel32".}
    result = int(GetTickCount64())
  else:
    var ts = default(Timespec)
    discard clock_gettime(CLOCK_MONOTONIC, ts)
    result = int(ts.tv_sec) * 1000 + int(ts.tv_nsec) div 1_000_000

proc progressSum(): int =
  ## Total tasks ever dequeued from the shared stripes and the inject queue.
  ## Relaxed loads, no lock: enqueues and lock contention neither hide nor fake
  ## a change. Only dequeues count.
  result = atomicLoad(injectQueue.dequeued, moRelaxed)
  for i in 0 ..< workerCount:
    result += atomicLoad(localQueues[i].dequeued, moRelaxed)

# --- Submit / dequeue ---

proc tryEnqueue(s: int; t: Task): bool {.inline.} =
  ## Push `t` onto stripe `s` if it has room; `false` when the stripe is full.
  let i = s mod workerCount
  return localQueues[i].tryEnqueue(t)

proc tryEnqueueAny(t: Task; h: int): bool {.inline.} =
  ## Stripes (hinted one first), then the inject queue.
  if tryEnqueue(h, t): return true
  for off in 1 ..< workerCount:
    workerMetrics[threadIdx].enqueueMiss += 1
    if tryEnqueue(h + off, t): return true
  result = injectQueue.tryEnqueue(t)

proc submit*(t: Task; h = 0) =
  ## Submit a task to the pool. **Non-lossy with backpressure:** try the hinted
  ## stripe, then the others (absorbing bursts), then the inject queue. If
  ## *every* queue is full:
  ##
  ## * on a **worker**, run the continuation inline ("caller-runs") —
  ##   trampolining it on the calling thread and handing the remainder back to
  ##   the pool the moment a slot frees — rather than dropping it or blocking.
  ## * on a **non-worker** while the pool is live (`poolLive`), wait for a slot
  ##   (spin, yield, then 50 us naps) and enqueue. A foreign thread does not run
  ##   shared pool tasks inline: they may take locks or arm I/O that only the
  ##   pool is meant to touch. It does so only if the pool looks wedged: no
  ##   queue has dequeued anything for `NonWorkerStallMs` (see `StallClock`).
  ##   Then it runs the task inline as before.
  ##
  ## Why workers cannot block: they re-submit continuations from inside the
  ## trampoline (see `workerLoop`). A blocking `submit` could park every worker
  ## on a full queue with none left to drain it -> deadlock. Caller-runs
  ## instead guarantees forward progress, so the queue stays bounded at
  ## `StripeCount*StripeSize`.
  ##
  ## Why a non-worker's wait ends: a worker can itself wait on the non-worker
  ## (for example every worker gated on a flag the non-worker sets after its
  ## submit returns), so a slot need not ever free. The stall check bounds the
  ## wait: if nothing dequeues for `NonWorkerStallMs` the non-worker falls back
  ## to caller-runs, which is what every program relied on before. Without live
  ## workers (`poolLive` false) it does so at once.
  workerMetrics[threadIdx].tasksSubmited += 1
  if tryEnqueueAny(t, h):
    if not isWorker: noteEnqueued()
    return
  if not isWorker and poolLive():
    var spins = 0
    while true:
      # The attempt just failed: sample the dequeue counter first.
      let sum = progressSum()
      let now = monoMs()
      if not submitClock.armed:
        submitClock = StallClock(armed: true, stallStart: now, lastSum: sum)
      elif sum != submitClock.lastSum:
        submitClock.lastSum = sum
        submitClock.stallStart = now
      elif now - submitClock.stallStart >= NonWorkerStallMs:
        discard atomicFetchAdd(nonWorkerStallRuns, 1, moRelaxed)
        break
      if spins < 64: cpuRelax()
      elif spins < 128: yieldThread()
      else: napMicros(50)
      inc spins
      if tryEnqueueAny(t, h):
        noteEnqueued()
        return
      if not poolLive(): break
  # Saturated: run it here. `c.fn` returns the next continuation, or one whose
  # `fn` is nil when the task completes or parks (I/O will resume a parked one).
  var c = t.con
  while true:
    let next = c.fn(c.env)
    if next.fn == nil: break
    if tryEnqueue(h, toTask(next)): break
    c = next

# --- Per-worker staging ring ---

type
  StagedTasks = object
    ## A worker's private submission ring: tasks submitted with the default
    ## hint land here (no locks; the ring is only touched by its owning
    ## thread) and are moved into the shared stripes in bulk at the top of
    ## the next `workerLoop` cycle. Turns bursts of resubmissions from inside
    ## the trampoline into one lock acquisition per `BulkSize` chunk.
    buf: array[StageSize, Task]
    head: int  # oldest element
    count: int

var
  staged {.threadvar.}: StagedTasks

proc stageTask(t: Task): bool {.inline.} =
  ## Push onto this worker's private ring; false when it is full.
  if staged.count == StageSize: return false
  staged.buf[(staged.head + staged.count) and (StageSize - 1)] = t
  inc staged.count
  result = true

proc flushStaged(hint: int): bool =
  ## Bulk-enqueue staged tasks into the shared stripes in FIFO order, hinted
  ## stripe first, one lock acquisition per chunk of up to `BulkSize` tasks.
  ## Whatever the stripes cannot absorb goes through `submit`'s caller-runs
  ## path so the batch stays loss-free. Returns true if any task was moved.
  result = false
  var chunk {.noinit.}: array[BulkSize, Task]
  while staged.count > 0:
    result = true
    let n = min(staged.count, BulkSize)
    for i in 0 ..< n:
      chunk[i] = staged.buf[staged.head]
      staged.head = (staged.head + 1) and (StageSize - 1)
    dec staged.count, n
    var remaining = n
    var off = 0
    while remaining > 0 and off < workerCount:
      let s = (hint + off) mod workerCount
      let moved = localQueues[s].tryBulkEnqueue(
        toOpenArray(chunk, n - remaining, n - 1))
      dec remaining, moved
      if moved == 0:
        workerMetrics[threadIdx].enqueueMiss += 1
      inc off
    while remaining > 0:
      let t = chunk[n - remaining]
      dec remaining
      submit(t, hint)

proc submit*(c: Continuation; hint = -1) {.inline.} =
  ## Convenience: submit a bare continuation as a task. On a worker with the
  ## default hint, the task is first staged on the private lock-free ring and
  ## handed to the shared stripes in bulk on the next worker cycle (see
  ## `flushStaged`); everything else enqueues immediately.
  if hint != -1 or not isWorker:
    var hint = hint
    if hint == -1:
      hint = threadIdx
    submit(toTask(c), hint)
  elif not stageTask(toTask(c)):
    # Ring full: drain it into the stripes, then stage again — after a full
    # flush there is always room.
    discard flushStaged(threadIdx)
    discard stageTask(toTask(c))

var dequeTicks {.threadvar.}: int

proc tryBulkDequeue(stripe: int; buf: var array[BulkSize, Task]): int =
  result = 0
  let s = stripe mod workerCount
  inc dequeTicks
  if dequeTicks mod 61 == 0:
    result = injectQueue.tryBulkDequeue(BulkSize, buf)
  if result == 0:
    result = localQueues[s].tryBulkDequeue(BulkSize, buf)

proc drainOnce(startStripe: int): bool =
  ## Dequeue the first non-empty stripe (searching from `startStripe`, for
  ## locality) and trampoline its tasks on the calling thread, re-submitting any
  ## continuation that yields more work. Returns true if a batch ran. Shared by
  ## the worker loop and `pool.help`.
  var buf {.noinit.}: array[BulkSize, Task]
  # 0. Publish staged tasks first so submissions made since the last cycle
  #    become visible to the scan below.
  discard flushStaged(startStripe)
  for attempt in 0 ..< workerCount:
    let n = tryBulkDequeue(startStripe + attempt, buf)
    if n > 0:
      for i in 0 ..< n:
        let c = buf[i].con
        let next = c.fn(c.env)
        if next.fn != nil:
          submit(next, startStripe)
      workerMetrics[threadIdx].tasksHandled += n.uint
      if attempt != 0:
        workerMetrics[threadIdx].tasksStealed += n.uint
      return true
    else:
      workerMetrics[threadIdx].dequeueMiss += 1
  result = false

proc poolQueueStats*(): tuple[queued, capacity, injected: int] =
  ## Tasks currently sitting in the shared stripes and the inject queue
  ## (`queued`, of which `injected` are in the inject queue), and the number
  ## the queues can hold in all (`capacity`). Takes each stripe's lock; meant
  ## for tests and diagnostics, not for hot paths.
  result = (queued: 0, capacity: 0, injected: 0)
  for i in 0 ..< workerCount:
    acquireSys(localQueues[i].lock)
    result.queued += localQueues[i].count
    result.capacity += localQueues[i].data.len
    releaseSys(localQueues[i].lock)
  acquireSys(injectQueue.lock)
  result.queued += injectQueue.count
  result.injected = injectQueue.count
  result.capacity += injectQueue.data.len
  releaseSys(injectQueue.lock)

proc poolHelp*(): bool {.inline.} =
  drainOnce(0)

when defined(useMimalloc):
  proc miCollect(force: bool) {.importc: "mi_collect".}
    ## mimalloc heap collection for the CALLING thread. Continuation-based
    ## scheduling constantly allocates on one worker and frees on another;
    ## those cross-thread frees land on the allocating heap's remote list and
    ## are only reclaimed when its owner thread collects. Without a periodic
    ## collect, that backlog grows without bound (measured: a passive proc
    ## owning an 8MB seq across one park leaks ~the full seq per invocation;
    ## flat when alloc+free stay on one thread — see harness
    ## tests/leak_repro*.nim). An idle-time collect converges it to a small
    ## per-worker steady state.

proc reactorReady*(): bool {.inline.} =
  ## Has the I/O ring published its reactor? See `gReactorState`.
  atomicLoad(gReactorState, moAcquire) != 0

proc reactorPoll*(timeoutMs: int): bool {.inline.} =
  ## Drive the I/O reactor if there is one, and the do-nothing default until
  ## there is. Every caller of `gReactor` goes through this.
  if reactorReady(): gReactor(timeoutMs) else: poolPollIo(timeoutMs)

proc workCycle(stripe: int): bool =
  ## One turn of a worker: run the queued tasks, drive this thread's I/O lane,
  ## and nap when neither had anything. Answers whether tasks ran.
  # 1. Bulk-drain tasks: own stripe first, then steal from others. Trampolines
  #    each continuation, re-submitting any that yield more work.
  result = drainOnce(stripe)
  # 2. Poll I/O — non-blocking when we just ran work, 1ms wait when idle.
  let eventFired = reactorPoll(if result: 0.cint else: 1.cint)
  if not eventFired and not result and not gReactorWaits:
    # Only when the reactor did not wait for us. Behind one that did, this
    # nap is the difference between noticing a completion when it arrives
    # and noticing it up to a millisecond later.
    const timeoutMs = 1
    when defined(windows):
      sleep(timeoutMs.uint32)
    else:
      var ts = Timespec(tv_sec: Time(timeoutMs div 1000),
                         tv_nsec: clong((timeoutMs mod 1000) * 1_000_000))
      var rem = Timespec()
      discard nanosleep(ts, rem)

# --- Non-worker waits ---
#
# A thread that is not a pool worker and waits for a parked `.passive` call
# (`complete`) polls only its OWN I/O lane and does not pop tasks off the shared
# stripes while the pool is live: those tasks belong to the workers (they may
# take locks, or arm I/O on the lane of the thread that runs them), and a
# foreign thread running one would put its continuation on a lane no worker
# drains. The exception is a stalled pool: when no shared queue has dequeued
# anything for `NonWorkerStallMs`, the waiter runs one batch per call as it did
# before (`waitStalled`). See doc/passive_procs.md, "Non-worker waits".

var
  inOwnWait {.threadvar.}: bool
    ## True only while a live-pool non-worker `passiveWait` is inside its own
    ## `reactorPoll`. Gates `deferOwnReady`.
  ownReady {.threadvar.}: seq[Continuation]
    ## Continuations of ops this non-worker submitted on its own lane that
    ## completed during its `passiveWait` poll. Run by that `passiveWait` after
    ## the poll returns, never inside it (no reactor re-entry).
  ownRedirects {.threadvar.}: int
  idleCalls {.threadvar.}: int
    ## Consecutive fruitless non-worker `passiveWait` calls; picks the back-off.

const
  SpinCalls = 10    ## calls that only spin (~2 us each) ~ 20 us
  YieldCalls = 40   ## calls that yield the CPU once each ~ 200 us
  RelaxPerCall = 32

proc deferOwnReady*(c: Continuation): bool =
  ## Called by the I/O backend when an op with continuation `c` completes. On a
  ## non-worker inside its own `passiveWait` poll the continuation is kept for
  ## that thread (returns true); everywhere else the caller submits it to the
  ## pool as before (returns false).
  if inOwnWait and not isWorker:
    ownReady.add c
    inc ownRedirects
    result = true
  else:
    result = false

proc ownWaitRedirects*(): int =
  ## Test seam: how many completions this thread has kept via `deferOwnReady`.
  ownRedirects

proc inOwnWaitNow*(): bool =
  ## Test seam: is this thread inside a non-worker `passiveWait` poll?
  inOwnWait

proc ownLanePoll(timeoutMs: cint): bool =
  ## Poll this thread's own lane, then run the continuations that completed
  ## there. Answers whether anything happened.
  assert not inOwnWait
  inOwnWait = true
  result = reactorPoll(timeoutMs)
  inOwnWait = false
  # The flag is clear from here on, so a nested pumpIo/closeFd/backpressure
  # poll in a continuation below submits to the pool as it always did. A
  # nested `passiveWait` drains whatever it adds before it returns.
  while ownReady.len > 0:
    var taken: seq[Continuation] = @[]
    swap(taken, ownReady)
    result = true
    for i in 0 ..< taken.len:
      var c = taken[i]
      while c.fn != nil:
        c = c.fn(c.env)

proc waitStalled(): bool =
  ## Late-phase sample of the pool's dequeue counter (see `StallClock`): true
  ## when it has not moved for `NonWorkerStallMs` of this wait.
  let now = monoMs()
  let sum = progressSum()
  if waitClock.armed and now - waitClock.lastSample > 5:
    waitClock.armed = false       # a gap between hook calls: a new wait
  if not waitClock.armed:
    waitClock = StallClock(armed: true, stallStart: now, lastSum: sum, lastSample: now)
    return false
  if sum != waitClock.lastSum:
    waitClock.lastSum = sum
    waitClock.stallStart = now
  waitClock.lastSample = now
  result = now - waitClock.stallStart >= NonWorkerStallMs

proc nonWorkerWait() =
  let n = idleCalls
  let late = n >= SpinCalls + YieldCalls
  if ownLanePoll(if late: 1.cint else: 0.cint):
    idleCalls = 0
    waitClock.armed = false
    return
  inc idleCalls
  if n < SpinCalls:
    for _ in 0 ..< RelaxPerCall: cpuRelax()
  elif n < SpinCalls + YieldCalls:
    yieldThread()
  elif waitStalled():
    # Nothing dequeued anywhere for NonWorkerStallMs: the workers may be waiting
    # on us. Help as before, one batch per call while the pool stays wedged.
    inStallDrain = true
    let ran = drainOnce(0)
    inStallDrain = false
    if ran:
      discard atomicFetchAdd(nonWorkerStallRuns, 1, moRelaxed)
    elif not gReactorWaits:
      napMicros(1000)
    # Our own dequeues are not progress; a worker dequeue in this window is
    # absorbed (harmless). The back-off counter and stallStart stay.
    waitClock.lastSum = progressSum()
    waitClock.lastSample = monoMs()
  elif not gReactorWaits:
    napMicros(1000)

proc passiveWait() {.nimcall.} =
  ## `system.setPassiveWaitHook`: a thread waiting for a parked `.passive` call
  ## to finish keeps the pool moving. A worker does a worker's turn: a worker
  ## that waited idly could be the one the pool needed to resume the callee.
  ##
  ## A non-worker polls its own lane only (its I/O ops complete nowhere else,
  ## see `ioLane`) and leaves the shared stripes to the workers as long as the
  ## pool keeps dequeuing. Workers may themselves wait on a non-worker, so if no
  ## queue has dequeued anything for `NonWorkerStallMs` it runs one batch per
  ## call as before (`nonWorkerStallRuns`), and if no worker is alive
  ## (`poolLive`) it does a worker's turn at once: nothing that progressed
  ## before hangs now.
  if isWorker:
    discard workCycle(threadIdx)
  elif poolLive():
    nonWorkerWait()
  else:
    discard workCycle(0)

proc workerLoop(arg: pointer) {.nimcall.} =
  threadIdx = cast[int](arg)
  isWorker = true
  var idleTicks = 0
  dequeTicks = 0
  var sinceCollect = 0
  while not atomicLoad(stopFlag, moRelaxed):
    let busy = workCycle(threadIdx)
    # 3. Reclaim this worker's cross-thread-free backlog: a forced collect
    #    after a brief idle (~8ms of 1ms polls), plus a hard periodic fallback
    #    so a worker that never goes idle still collects. force=true is what
    #    actually drains the remote list; at this cadence its cost is noise.
    #    (mimalloc-only: the default nimNativeAlloc has its own cross-thread
    #    free path — measure before assuming it needs an equivalent.)
    when defined(useMimalloc):
      inc sinceCollect
      if busy: idleTicks = 0 else: inc idleTicks
      if idleTicks >= 8 or sinceCollect >= 8192:
        idleTicks = 0
        sinceCollect = 0
        miCollect(true)
  discard atomicFetchSub(liveWorkers, 1, moRelaxed)

# --- Lifecycle ---

var poolState: int

proc initPool*(count = 0) =
  ## Start the worker pool: `count` threads, or one fewer than
  ## `countProcessors()` when `count` is 0. Idempotent: once a pool is up,
  ## later calls return at once and their `count` is ignored. Workers are not
  ## pinned to CPUs; the scheduler places them within the process's affinity.
  # Only `poolState` gates this; `workerCount` must NOT, because the CAS winner
  # publishes it *before* allocating `workerMetrics`/`localQueues`/`injectQueue`.
  # A second caller that returned on `workerCount > 0` would race ahead and index
  # those still-empty seqs on its next `submit`.
  if atomicLoad(poolState, moAcquire) == 2: return
  var expected = 0
  if atomicCompareExchange(poolState, expected, 1):
    workerCount = if count > 0: count else: max(1, cpuinfo.countProcessors() - 1)
    setPassiveWaitHook passiveWait
    workerMetrics = newSeq[WorkerMetrics](workerCount)
    localQueues = newSeq[FifoStripe[Task]](workerCount)
    for i in 0..<workerCount:
      localQueues[i].init(StripeSize)
    injectQueue.init(StripeSize*16)
    workers.setLen(workerCount)
    let toStart = if poolTestWorkerLimit >= 0: min(poolTestWorkerLimit, workerCount)
                  else: workerCount
    # Before the first thread exists, so a worker that exits at once cannot
    # drive the count negative; a failed `create` gives its share back.
    atomicStore(liveWorkers, toStart, moRelaxed)
    for i in 0 ..< toStart:
      try:
        create workers[i], workerLoop, cast[pointer](i)
      except:
        discard atomicFetchSub(liveWorkers, 1, moRelaxed)
    atomicStore(poolState, 2, moRelease)
  else:
    while atomicLoad(poolState, moAcquire) != 2:
      discard

proc isPoolWorker*(): bool {.inline.} =
  ## True on a thread created by `initPool`. `threadIdx` is only meaningful as
  ## an identity on those: it is a threadvar defaulting to 0, so every foreign
  ## thread (the main thread included) reports 0 — the same value worker 0 uses.
  ## Anything that indexes per-thread state by `threadIdx` must ask this first.
  isWorker

proc stopped*(): bool {.inline.} =
  atomicLoad(stopFlag, moRelaxed)

proc shutdownPool*() =
  atomicStore(stopFlag, true, moRelaxed)
  for i in 0 ..< workerCount:
    workers[i].join()
