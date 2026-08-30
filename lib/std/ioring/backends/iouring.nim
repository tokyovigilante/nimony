# Linux io_uring backend.
# Uses the existing Queue from lib/std/posix/io_uring.nim.
# Does not use PollBackend since io_uring uses its own submission/completion
# queue model.
#
# Submissions are deferred to a shared queue so that the calling thread
# (e.g. main) never owns an SQE — every SQE is filled and flushed on the
# thread that calls poll(), which is always a worker thread (or whomever
# calls waitCompletions()). That avoids the "submitted on main, never
# polled" hang.

import std/[assertions, atomics, posix/posix, ticketlocks, threadpool]
import std/syncio
import ../../posix/io_uring
import ../core/types
import ../core/slots
import ../core/backend
from ./epoll import initEpollBackendRelays

const
  DrainBatch = 128  ## Max deferred entries drained per poll() call.
  # Standard poll(2) mask bits used by io_uring's OP_POLL_ADD (distinct from
  # the internal EvRead/EvWrite flags, which never cross into the kernel).
  POLLIN = uint32(0x0001)
  POLLOUT = uint32(0x0004)

const TunedSetupFlags: SetupFlags =
  when defined(nimIoringPlainSetup): {}
  else: {SETUP_SINGLE_ISSUER, SETUP_COOP_TASKRUN}
  ## What a worker lane's ring is created with. Every ring used to be created by
  ## `newQueue(sqEntries)` — `defaultFlags`, i.e. `{}` — so the backend had
  ## completion semantics instead of readiness semantics and none of the
  ## behaviour that makes completions worth having.
  ##
  ## `SINGLE_ISSUER` declares what this backend already guarantees: a lane's ring
  ## is submitted by exactly one thread. `COOP_TASKRUN` drops the per-completion
  ## IPI, letting completion work run at the next ring transition instead. Both
  ## are measured neutral on the loopback WebSocket matrix (27.9 MB/s, p95 159
  ## against a no-flags control of 27.9 / 160) — enabled because they are
  ## honest descriptions of this backend, not because loopback showed a win.
  ##
  ## `SINGLE_ISSUER` binds the ring to its submitter **when the ring is
  ## CREATED**, which is why `adoptLane` exists.
  ##
  ## **`DEFER_TASKRUN` is not here yet, and not for lack of trying.** It is the
  ## flag that should matter most — completion work would run when the owning
  ## task next calls `io_uring_enter` rather than being pushed to it through
  ## task_work — and `SINGLE_ISSUER` is its prerequisite, so it is now within
  ## reach for the first time. Enabling it measures **0.8 MB/s against 27.9**,
  ## with 94% of reader polls finding an empty socket: completions are barely
  ## being reaped. Hoisting the `submit` out of the `n > 0` branch so that a poll
  ## with nothing to submit still enters — the obvious first suspect, since
  ## deferred work needs an enter to run — **does not fix it**, so something
  ## further in the poll loop's contract with deferred completions is wrong and
  ## is not yet diagnosed. Do not enable it without re-running the matrix.
  ##
  ## NOT `SQPOLL`: one kernel polling thread **per ring**, and we create
  ## `ioLanes()` rings — `workerCount + 1` kernel threads spinning, which on a
  ## core-bound host costs more than it saves.
  ##
  ## `-d:nimIoringPlainSetup` restores the old empty set, so a regression can be
  ## bisected to this decision rather than to the backend as a whole.

var
  sqEntries: int
  localQueues: seq[Queue]
  laneAdopted: seq[bool]
    ## Whether a lane's ring has been re-created by the thread that owns it.
    ## Written only by that thread, on its first poll, and read by nobody else.

proc tryInitLocalQueues(): bool =
  ## Create every lane's ring PLAINLY, on whatever thread is calling. Worker
  ## lanes are re-created later by their own thread — see `adoptLane`.
  localQueues = @[]
  laneAdopted = newSeq[bool](ioLanes())
  try:
    while localQueues.len < ioLanes():
      localQueues.add newQueue(sqEntries, {})
  except ErrorCode as e:
    stderr.writeLine("ioring: failed to init io_uring queue: " & $e)
    return false
  return true

proc adoptLane(lane: int) {.inline.} =
  ## Re-create this lane's ring on the thread that will own it, once.
  ##
  ## **Why a ring is thrown away to do this.** `SINGLE_ISSUER` — and so
  ## `DEFER_TASKRUN`, which requires it — fixes the ring's submitting task when
  ## `io_uring_setup` runs, not when the ring is first used. Rings are created in
  ## `tryInitLocalQueues` on whichever thread called `initLoop()`, while a worker
  ## lane is submitted only ever by its own worker, so a ring created there and
  ## submitted here is refused. Creating it here instead is the whole fix:
  ## measured with `SINGLE_ISSUER`, a 4-connection cell went from **0.0 MB/s with
  ## 100% of reader polls finding an empty socket** to 27.9 MB/s at p95 159 —
  ## indistinguishable from the no-flags control.
  ##
  ## **Why replace rather than create lazily into an empty slot.** A `Queue` owns
  ## a fd and three mappings and has no valid zero state — `newSeq[Queue]` cannot
  ## build one — so `seq[Queue]` cannot carry a hole. Assigning over the slot
  ## destroys the plain ring and installs the owned one, which costs one unused
  ## ring per worker at startup and keeps the type honest everywhere else.
  ##
  ## The trailing lane is deliberately skipped: `ioLane()` hands it to every
  ## non-worker submitter, so no single thread can claim it and `SINGLE_ISSUER`
  ## there would be a promise we cannot keep.
  ##
  ## A refusal (kernel too old — `SINGLE_ISSUER` needs 6.0, `DEFER_TASKRUN` 6.1,
  ## `COOP_TASKRUN` 5.19, and `io_uring_setup` answers `-EINVAL` rather than
  ## ignoring an unknown flag) simply leaves the plain ring in place.
  if not laneAdopted[lane]:
    laneAdopted[lane] = true
    when not defined(nimIoringPlainSetup):
      if lane < workerCount:
        try:
          localQueues[lane] = newQueue(sqEntries, TunedSetupFlags)
        except ErrorCode:
          discard

proc fillSqe(sqe: ptr Sqe; op: ptr OpContext) {.inline.} =
  case op.kind
  of opRead:
    if op.buf != nil:
      discard sqe.read(op.fd, cast[pointer](op.buf), op.len)
  of opWrite:
    if op.buf != nil:
      discard sqe.write(op.fd, cast[pointer](op.buf), op.len)
  of opAccept:
    discard sqe.accept(SocketHandle(op.fd), cast[ptr SockAddr](addr op.acceptAddr), addr op.acceptLen, 0)
  of opPollAdd:
    # Single-shot readiness probe on the direction(s) the caller asked for;
    # completes with the fired poll mask, then the slot is freed so the caller
    # re-arms with a new submitPollAdd (matching the epoll/kqueue oneshot
    # behaviour). Watching both regardless would spin a read-waiter on a
    # writable socket — see submitPollAdd's docstring.
    var pollFlags = 0'u32
    if (op.pollMask and EvRead) != 0: pollFlags = pollFlags or POLLIN
    if (op.pollMask and EvWrite) != 0: pollFlags = pollFlags or POLLOUT
    discard sqe.poll_add(op.fd, pollFlags)
  of opNop:
    discard sqe.nop()

proc iouringPoll(timeoutMs: int): bool {.nimcall.} =
  # Drain the shared deferred queue: for every pending slot, fill a fresh
  # SQE in THIS thread's io_uring instance. Only worker threads (and
  # callers of waitCompletions) poll, so all SQEs are always submitted
  # from within the poll loop that also reads their CQEs.
  #
  # Drain is bounded (DrainBatch) so a flood of submissions cannot keep a
  # worker inside poll() forever — the outer worker loop also runs task
  # draining, and remaining deferred entries are picked up next iteration.
  let lane = ioLane()
  adoptLane(lane)
  var buf {.noinit.}: array[DrainBatch, OpContext]
  var n = gOpQueues[lane].tryBulkDequeue(DrainBatch, buf)
  if n > 0:
    for i in 0..<n:
      var sqe: nil ptr Sqe
      try:
        sqe = localQueues[lane].getSqe()
      except ErrorCode as e:
        stderr.writeLine("ioring: failed to get sqe: " & $e)
        # Ops buf[i..<n] were dequeued but never got an SQE/slot; put them
        # back so the next poll picks them up instead of losing them forever.
        for k in i..<n:
          discard gOpQueues[lane].tryEnqueue(buf[k])
        break
      if sqe == nil:
        for k in i..<n:
          discard gOpQueues[lane].tryEnqueue(buf[k])
        break
      let idx = gSlots[lane].allocSlot(buf[i])
      sqe.userData = cast[pointer](uint(idx))
      # Fill from the ARENA copy, never from `buf`: an accept SQE stores
      # `addr op.acceptAddr`/`addr op.acceptLen` and the kernel writes through
      # those at completion time, long after this stack frame is gone.
      fillSqe(sqe, addr gSlots[lane].slots[idx].op)
    try:
      discard localQueues[lane].submit()
    except ErrorCode as e:
      quit "fatal: bug: submit cannot fail: " & $e
  if localQueues[lane].cqReady > 0:
    var cqes {.noinit.}: array[DrainBatch, Cqe]
    try:
      n = localQueues[lane].copyCqes(cqes)
    except ErrorCode as e:
      quit "fatal: bug: copyCqes cannot fail: " & $e
    if n > 0:
      for i in 0..<n:
        let idx = int(cqes[i].userData)
        # For OP_POLL_ADD the kernel reports the fired mask in poll(2) form;
        # translate it to the same internal EvRead/EvWrite flags the epoll/
        # kqueue backends use, so the completion's `result` is consistent no
        # matter which backend is in use.
        var res = int(cqes[i].res)
        if gSlots[lane].slots[idx].op.kind == opPollAdd:
          var ev = 0
          if (uint32(res) and POLLIN) != 0:
            ev = ev or EvRead
          if (uint32(res) and POLLOUT) != 0:
            ev = ev or EvWrite
          res = ev
        complete(idx, res)
      return true
  return false

proc iouringForgetFd(fd: cint) {.nimcall.} =
  discard

proc iouringClose() {.nimcall.} =
  for q in localQueues:
    if q.params != nil:
      teardown(q)

proc initIoUringBackendRelays*(sqE = 256): BackendRelays =
  sqEntries = sqE
  if not tryInitLocalQueues():
    return initEpollBackendRelays()
  result = BackendRelays(
    poll: iouringPoll,
    close: iouringClose,
    forgetFd: iouringForgetFd,
  )
