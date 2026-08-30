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
  else: {SETUP_COOP_TASKRUN}
  ## What a worker lane's ring is created with. Every ring used to be created by
  ## `newQueue(sqEntries)` — i.e. `defaultFlags`, i.e. `{}` — so the backend had
  ## completion semantics instead of readiness semantics and none of the
  ## behaviour that makes completions worth having.
  ##
  ## `COOP_TASKRUN` asks the kernel not to interrupt the owning task with an IPI
  ## for every completion, letting them be run at the next ring transition
  ## instead. It carries no constraint on who may submit, which is why it is the
  ## only one here.
  ##
  ## **`SINGLE_ISSUER` and `DEFER_TASKRUN` are deliberately absent, and it is not
  ## because they do not apply.** `DEFER_TASKRUN` is the flag that should matter
  ## most for this backend — completion work would run when the owning task next
  ## calls `io_uring_enter`, which is precisely what `iouringPoll` does every
  ## pass — and it requires `SINGLE_ISSUER`. But `SINGLE_ISSUER` binds the ring
  ## to a single submitting task, and the kernel fixes that task **when the ring
  ## is created**, not when it is first used. These rings are created in
  ## `tryInitLocalQueues`, on whichever thread called `initLoop()`, while each
  ## worker lane is submitted from its own worker thread. Every worker's enter is
  ## then refused.
  ##
  ## Measured: with `SINGLE_ISSUER` alone, a 4-connection WebSocket cell
  ## delivered **0.0 MB/s** with 100% of reader polls finding an empty socket and
  ## both socket queues empty — a total stall, and a **silent** one, which is the
  ## worse half.
  ##
  ## Enabling them therefore means moving ring creation onto the thread that will
  ## own it: each worker creates its lane's ring lazily on first use. That is a
  ## backend change, not a flag, and it is the prerequisite for ever measuring
  ## whether `DEFER_TASKRUN` helps us.
  ##
  ## NOT `SQPOLL` either: it runs one kernel polling thread **per ring**, and we
  ## create `ioLanes()` rings — `workerCount + 1` kernel threads spinning, which
  ## on a core-bound host costs more than it saves.
  ##
  ## `-d:nimIoringPlainSetup` restores the old empty set, so a regression can be
  ## bisected to this decision rather than to the backend as a whole.

var
  sqEntries: int
  localQueues: seq[Queue]

proc tryInitLocalQueues(): bool =
  localQueues = @[]
  # Whether the tuned flags are available is a property of the KERNEL, not of
  # the lane, so it is asked once rather than `ioLanes()` times. `SINGLE_ISSUER`
  # needs 6.0, `DEFER_TASKRUN` 6.1 and `COOP_TASKRUN` 5.19, and `io_uring_setup`
  # answers an unsupported flag with -EINVAL rather than ignoring it — so asking
  # unconditionally would turn a working older kernel into a hard startup
  # failure. The probe ring is not thrown away: lane 0 is a worker lane, so this
  # is exactly the ring lane 0 wanted.
  var flags: SetupFlags = {}
  if workerCount > 0:
    try:
      localQueues.add newQueue(sqEntries, TunedSetupFlags)
      flags = TunedSetupFlags
    except ErrorCode:
      discard
  try:
    while localQueues.len < ioLanes():
      # Worker lanes have exactly one submitting thread. The TRAILING lane is
      # shared by every non-worker submitter (see `ioLane`), so it gets none of
      # the flags — `SINGLE_ISSUER` there would be a claim we cannot keep.
      var f: SetupFlags = {}
      if localQueues.len < workerCount: f = flags
      localQueues.add newQueue(sqEntries, f)
  except ErrorCode as e:
    stderr.writeLine("ioring: failed to init io_uring queue: " & $e)
    return false
  return true

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
