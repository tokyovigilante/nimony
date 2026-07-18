# (c) 2025 Andreas Rumpf
# Lock-striped thread pool with continuation-based scheduling.
#
# Workers contend on independent stripes to reduce lock pressure.
# Each worker polls I/O every iteration; the timeout doubles as idle sleep.
# Supports epoll (Linux), kqueue (macOS/BSD), or plain sleep fallback.
#
# A Task wraps a Continuation plus metadata. The pool schedules Tasks;
# the worker trampolines the inner continuation.

{.feature: "lenientnils".}

import std / [atomics, rawthreads, assertions, ticketlocks]

when defined(linux):
  const hasEpoll = true
  const hasKqueue = false
elif defined(macosx) or defined(freebsd) or defined(netbsd) or
     defined(openbsd) or defined(dragonfly):
  const hasEpoll = false
  const hasKqueue = true
else:
  const hasEpoll = false
  const hasKqueue = false

const hasWsaPoll = defined(windows)  ## Winsock WSAPoll readiness (client-scale).
const hasIoPoll* = hasEpoll or hasKqueue or hasWsaPoll

when defined(windows):
  import windows/winlean  # sleep(); the WSAPoll arm below binds Winsock itself
elif not hasIoPoll:
  proc usleepMicroseconds(usec: cuint): cint {.importc, header: "<unistd.h>".}

# --- Epoll bindings ---

when hasEpoll:
  const
    EPOLLIN* = 0x001'u32
    EPOLLOUT* = 0x004'u32
    EPOLLONESHOT* = 1'u32 shl 30

  type
    EpollData {.importc: "epoll_data_t", header: "<sys/epoll.h>".} = object
      p* {.importc: "ptr".}: pointer

    EpollEvent* {.importc: "struct epoll_event", header: "<sys/epoll.h>".} = object
      events*: uint32
      data*: EpollData

  const
    EPOLL_CTL_ADD* = 1.cint
    EPOLL_CTL_DEL* = 2.cint
    EPOLL_CTL_MOD* = 3.cint

  proc epoll_create1(flags: cint): cint {.importc, header: "<sys/epoll.h>".}
  proc epoll_ctl(epfd: cint; op: cint; fd: cint; event: ptr EpollEvent): cint {.
    importc, header: "<sys/epoll.h>".}
  proc epoll_wait(epfd: cint; events: ptr EpollEvent; maxevents: cint;
                  timeout: cint): cint {.importc, header: "<sys/epoll.h>".}

# --- Kqueue bindings ---

when hasKqueue:
  when not declared(Time):
    when defined(linux):
      type Time = clong
    else:
      type Time = int

  type
    Timespec {.importc: "struct timespec", header: "<time.h>".} = object
      tv_sec: Time
      tv_nsec: clong

  const
    EVFILT_READ* = cshort(-1)
    EVFILT_WRITE* = cshort(-2)
    EV_ADD* = cushort(0x0001)
    EV_DELETE* = cushort(0x0002)
    EV_ONESHOT* = cushort(0x0010)
    EV_ENABLE* = cushort(0x0004)

  type
    KEvent* {.importc: "struct kevent", header: "<sys/event.h>".} = object
      ident*: csize_t     ## identifier for this event (uintptr_t)
      filter*: cshort     ## filter for event
      flags*: cushort     ## action flags for kqueue
      fflags*: cuint      ## filter flag value
      data*: int          ## filter data value
      udata*: pointer     ## opaque user data identifier

  proc kqueue*(): cint {.importc, header: "<sys/event.h>".}
  proc kevent*(kq: cint; changelist: ptr KEvent; nchanges: cint;
               eventlist: ptr KEvent; nevents: cint;
               timeout: ptr Timespec): cint {.importc, header: "<sys/event.h>".}

# --- WSAPoll bindings (Windows) ---

when hasWsaPoll:
  # Winsock's poll(). Bound by `dynlib` rather than a `<winsock2.h>` include so
  # codegen never has to interleave that header with winlean's `<Windows.h>`
  # (whose implicit legacy `<winsock.h>` would clash) — and matching winlean's
  # own all-dynlib style for Win32 syscalls. The struct is ABI-compatible with
  # `WSAPOLLFD` (SOCKET + two SHORTs, natural alignment), so we declare it
  # directly instead of importing the header type.
  #
  # Known caveat: on Windows builds before 10 (2004), WSAPoll fails to report a
  # failed connect() (no POLLERR is raised). Acceptable here — the readiness arm
  # targets current Windows for the hikaru client.
  type
    SocketHandle = uint
      ## Winsock `SOCKET` is `UINT_PTR`. The public readiness API is `cint`; we
      ## cast on the way in (registerFd) — for a client's handful of sockets the
      ## handle values fit a `cint` in practice. The proper fix is the
      ## fdslots/backend redesign — see hashi doc/iocp-ioring-briefing.md
      ## open question 1.
    WSAPOLLFD = object
      fd: SocketHandle
      events: cshort
      revents: cshort

  const
    POLLRDNORM = cshort(0x0100)  ## normal data readable
    POLLWRNORM = cshort(0x0010)  ## normal data writable
    POLLERR    = cshort(0x0001)  ## error condition (revents only)
    POLLHUP    = cshort(0x0002)  ## hang-up (revents only)
    POLLNVAL   = cshort(0x0004)  ## invalid socket (revents only)
    PollErrMask = int(POLLERR) or int(POLLHUP) or int(POLLNVAL)

  proc wsaStartup(wVersionRequested: uint16; lpWSAData: pointer): cint {.
    stdcall, importc: "WSAStartup", dynlib: "ws2_32.dll".}
  proc wsaPoll(fdArray: ptr WSAPOLLFD; nfds: culong; timeout: cint): cint {.
    stdcall, importc: "WSAPoll", dynlib: "ws2_32.dll".}

  proc getTickCount64(): uint64 {.
    stdcall, importc: "GetTickCount64", dynlib: "kernel32.dll".}
    ## Milliseconds since boot, monotonic. The base clock for the deadline timer.

# --- Unified event mask ---

const
  EvRead*  = 1u32  ## Readable event.
  EvWrite* = 4u32  ## Writable event.

# --- Configuration ---

const
  StripeCount* = 8    ## Must be a power of 2.
  StripeSize*  = 128  ## Tasks per stripe; must be a power of 2.
  WorkerCount* = 8
  MaxIoEvents  = 64
  BulkSize*    = 16   ## Max tasks drained per bulk dequeue.

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

type
  Stripe = object
    L: TicketLock
    head, tail, count: int
    data: array[StripeSize, Task]

  IoHandler* = object
    ## Heap-allocate and pass ptr to registerFd.
    ## Kept alive until unregisterFd is called.
    ## Embed as the first field of a larger struct to carry per-connection state,
    ## then cast `ptr IoHandler` back to your struct pointer inside the callback.
    fd*: cint
    cb*: proc (self: ptr IoHandler; events: uint32) {.nimcall.}

var
  stripes: array[StripeCount, Stripe]
  workers {.noinit.}: array[WorkerCount, RawThread]
  gIoFd: cint
  stopFlag: bool  # accessed atomically

when hasWsaPoll:
  # WSAPoll has no kernel-side registration set, so we keep our own. 64 entries
  # is ample: this serves a curl *client* (one server connection + occasional
  # HTTP), not a server's fanout. A slot is free when `handler == nil`.
  type
    WsaSlot = object
      fd: cint               ## public-API fd; meaningful only while handler != nil
      handler: ptr IoHandler
      events: uint32         ## armed readiness mask (EvRead/EvWrite); 0 = disarmed
  var
    gWsaSlots: array[64, WsaSlot]
    gWsaLock: TicketLock
    gWsaPolling: AtomicFlag  ## single-poller gate: only one thread runs WSAPoll
    # --- Deadline timer (poller-owned) ---
    # WSAPoll can't watch a timer, so curl's CURLMOPT_TIMERFUNCTION ("call me in
    # N ms") becomes poller-owned state consulted each worker tick (~1 kHz, so
    # ~1 ms firing granularity). v1 holds a single deadline — enough for one curl
    # multi handle; a small min-heap keyed on deadline would generalise to many
    # concurrent timers with the same fire-due-then-dispatch shape. Guarded by
    # gWsaLock, same as the slot table.
    gTimerDeadline: int64      ## 0 = disarmed; else absolute ms (getTickCount64 base)
    gTimerHandler: ptr IoHandler

# --- Submit / dequeue ---

proc tryEnqueue(s: int; t: Task): bool {.inline.} =
  ## Push `t` onto stripe `s` if it has room; `false` when the stripe is full.
  let i = s and (StripeCount - 1)
  stripes[i].L.acquire()
  result = stripes[i].count < StripeSize
  if result:
    stripes[i].data[stripes[i].tail] = t
    stripes[i].tail = (stripes[i].tail + 1) and (StripeSize - 1)
    inc stripes[i].count
  stripes[i].L.release()

proc submit*(t: Task; hint = 0) =
  ## Submit a task to the pool. **Non-lossy with "caller-runs" backpressure:**
  ## try the hinted stripe, then the others (absorbing bursts); if *every*
  ## stripe is full, run the continuation inline — trampolining it on the
  ## calling thread and handing the remainder back to the pool the moment a
  ## slot frees — rather than dropping it or blocking.
  ##
  ## Why caller-runs and not a blocking wait: workers re-submit continuations
  ## from inside the trampoline (see `workerLoop`). A blocking `submit` could
  ## park every worker on a full queue with none left to drain it -> deadlock.
  ## Caller-runs instead guarantees forward progress (a producer that outruns
  ## the pool simply does the work), so the queue stays bounded at
  ## `StripeCount*StripeSize` and no submitter ever stalls.
  let h = hint and (StripeCount - 1)
  if tryEnqueue(h, t): return
  for off in 1 ..< StripeCount:
    if tryEnqueue(h + off, t): return
  # Saturated: run it here. `c.fn` returns the next continuation, or one whose
  # `fn` is nil when the task completes or parks (I/O will resume a parked one).
  var c = t.con
  while true:
    let next = c.fn(c.env)
    if next.fn == nil: break
    if tryEnqueue(h, toTask(next)): break
    c = next

proc submit*(c: Continuation; hint = 0) {.inline.} =
  ## Convenience: submit a bare continuation as a task.
  submit(toTask(c), hint)

proc tryBulkDequeue(stripe: int; buf: var array[BulkSize, Task]): int =
  let s = stripe and (StripeCount - 1)
  stripes[s].L.acquire()
  result = min(stripes[s].count, BulkSize)
  for i in 0 ..< result:
    buf[i] = stripes[s].data[stripes[s].head]
    stripes[s].head = (stripes[s].head + 1) and (StripeSize - 1)
  dec stripes[s].count, result
  stripes[s].L.release()

proc drainOnce(startStripe: int): bool =
  ## Dequeue the first non-empty stripe (searching from `startStripe`, for
  ## locality) and trampoline its tasks on the calling thread, re-submitting any
  ## continuation that yields more work. Returns true if a batch ran. Shared by
  ## the worker loop and `poolHelp`.
  var buf {.noinit.}: array[BulkSize, Task]
  for attempt in 0 ..< StripeCount:
    let s = (startStripe + attempt) and (StripeCount - 1)
    let n = tryBulkDequeue(s, buf)
    if n > 0:
      for i in 0 ..< n:
        let c = buf[i].con
        let next = c.fn(c.env)
        if next.fn != nil:
          submit(next, s)
      return true
  result = false

proc poolHelp*(): bool {.inline.} =
  ## Run one batch of queued tasks on the calling thread. A thread blocked on a
  ## join (`parWait`) calls this instead of idle-spinning, so it *helps* drain
  ## the pool — without it, nested parallel regions deadlock once every worker
  ## is spin-waiting in a join with its sub-tasks unrun in the queue (fork-join
  ## work-donation). Returns true if any task ran.
  drainOnce(0)

# --- I/O registration ---

proc ioFd*(): cint {.inline.} = gIoFd
  ## The shared I/O poller file descriptor (epoll fd or kqueue fd).

proc perror(s: cstring) {.importc, header: "<stdio.h>".}
  ## libc perror — report a residual epoll_ctl failure on stderr (see the
  ## ADD/MOD fallbacks in registerFd/rearmFd).

proc registerFd*(fd: cint; handler: ptr IoHandler; events: uint32) =
  ## Register fd with the shared I/O instance.
  ## Oneshot semantics: exactly one worker handles each fired event.
  when hasEpoll:
    var mask = EPOLLONESHOT
    if (events and EvRead) != 0: mask = mask or EPOLLIN
    if (events and EvWrite) != 0: mask = mask or EPOLLOUT
    var ev = EpollEvent(events: mask)
    ev.data.p = handler
    if epoll_ctl(gIoFd, EPOLL_CTL_ADD, fd, addr ev) == -1:
      # Concurrent armers can race ADD vs MOD (the slot's `registered` flag is
      # advisory across workers). ADD on an already-present fd → EEXIST; fall
      # back to MOD so the fd ends up armed with the current mask instead of
      # staying a fired (disarmed) oneshot — that stall loses the connection.
      if epoll_ctl(gIoFd, EPOLL_CTL_MOD, fd, addr ev) == -1:
        perror("ioring: epoll ADD+MOD both failed")
  elif hasKqueue:
    var kevs = default array[2, KEvent]
    var n = 0
    if (events and EvRead) != 0:
      kevs[n].ident = fd.csize_t
      kevs[n].filter = EVFILT_READ
      kevs[n].flags = EV_ADD or EV_ONESHOT
      kevs[n].udata = handler
      inc n
    if (events and EvWrite) != 0:
      kevs[n].ident = fd.csize_t
      kevs[n].filter = EVFILT_WRITE
      kevs[n].flags = EV_ADD or EV_ONESHOT
      kevs[n].udata = handler
      inc n
    if n > 0:
      discard kevent(gIoFd, addr kevs[0], n.cint, nil, 0, nil)
  elif hasWsaPoll:
    gWsaLock.acquire()
    var slot = -1
    var free = -1
    for i in 0 ..< gWsaSlots.len:
      if gWsaSlots[i].handler != nil and gWsaSlots[i].fd == fd:
        slot = i
        break
      elif free < 0 and gWsaSlots[i].handler == nil:
        free = i
    if slot < 0: slot = free
    if slot < 0:
      gWsaLock.release()
      perror("ioring: WSAPoll registration table full")
    else:
      gWsaSlots[slot].fd = fd
      gWsaSlots[slot].handler = handler
      gWsaSlots[slot].events = events
      gWsaLock.release()

proc rearmFd*(fd: cint; handler: ptr IoHandler; events: uint32) =
  ## Re-arm a oneshot fd after it has fired.
  when hasEpoll:
    var mask = EPOLLONESHOT
    if (events and EvRead) != 0: mask = mask or EPOLLIN
    if (events and EvWrite) != 0: mask = mask or EPOLLOUT
    var ev = EpollEvent(events: mask)
    ev.data.p = handler
    if epoll_ctl(gIoFd, EPOLL_CTL_MOD, fd, addr ev) == -1:
      # Mirror of registerFd's fallback: MOD on an fd that isn't in the set
      # (ENOENT — e.g. armed-flag set before the racing ADD landed) → ADD.
      if epoll_ctl(gIoFd, EPOLL_CTL_ADD, fd, addr ev) == -1:
        perror("ioring: epoll MOD+ADD both failed")
  elif hasKqueue:
    var kevs = default array[2, KEvent]
    var n = 0
    if (events and EvRead) != 0:
      kevs[n].ident = fd.csize_t
      kevs[n].filter = EVFILT_READ
      kevs[n].flags = EV_ADD or EV_ONESHOT or EV_ENABLE
      kevs[n].udata = handler
      inc n
    if (events and EvWrite) != 0:
      kevs[n].ident = fd.csize_t
      kevs[n].filter = EVFILT_WRITE
      kevs[n].flags = EV_ADD or EV_ONESHOT or EV_ENABLE
      kevs[n].udata = handler
      inc n
    if n > 0:
      discard kevent(gIoFd, addr kevs[0], n.cint, nil, 0, nil)
  elif hasWsaPoll:
    # The `handler` arg is unused here: the WSAPoll table is keyed by fd (epoll
    # needs it to re-stamp data.p; we don't). Just refresh the armed mask.
    gWsaLock.acquire()
    for i in 0 ..< gWsaSlots.len:
      if gWsaSlots[i].handler != nil and gWsaSlots[i].fd == fd:
        gWsaSlots[i].events = events
        break
    gWsaLock.release()

proc unregisterFd*(fd: cint) =
  when hasEpoll:
    discard epoll_ctl(gIoFd, EPOLL_CTL_DEL, fd, nil)
  elif hasKqueue:
    var kevs = default array[2, KEvent]
    kevs[0].ident = fd.csize_t
    kevs[0].filter = EVFILT_READ
    kevs[0].flags = EV_DELETE
    kevs[1].ident = fd.csize_t
    kevs[1].filter = EVFILT_WRITE
    kevs[1].flags = EV_DELETE
    discard kevent(gIoFd, addr kevs[0], 2, nil, 0, nil)
  elif hasWsaPoll:
    gWsaLock.acquire()
    for i in 0 ..< gWsaSlots.len:
      if gWsaSlots[i].handler != nil and gWsaSlots[i].fd == fd:
        gWsaSlots[i].handler = nil
        gWsaSlots[i].events = 0
        break
    gWsaLock.release()

when hasWsaPoll:
  proc armPoolTimer*(handler: ptr IoHandler; delayMs: int64) =
    ## Windows only. Arm the poller-owned deadline timer: the pool fires
    ## `handler.cb(handler, 0)` exactly once, ~`delayMs` ms from now (checked each
    ## worker tick, so ~1 ms granularity). `delayMs < 0` disarms (curl's `-1`
    ## "no timeout" convention); `0` fires on the next tick. The callback may
    ## re-arm from inside its own dispatch (curl does) — the deadline is claimed
    ## and cleared before dispatch, so a re-arm inside the callback survives.
    ##
    ## POSIX has no equivalent: there a timerfd is registered through the ordinary
    ## `registerFd` readiness path instead (see harness `armTimerfd`). The `events`
    ## value passed to the callback is `0`, which distinguishes a timer expiry from
    ## fd readiness (always a non-zero EvRead/EvWrite mask).
    gWsaLock.acquire()
    if delayMs < 0:
      gTimerDeadline = 0
      gTimerHandler = nil
    else:
      gTimerDeadline = int64(getTickCount64()) + delayMs
      gTimerHandler = handler
    gWsaLock.release()

  proc disarmPoolTimer*() =
    ## Windows only. Cancel the deadline timer if armed. See `armPoolTimer`.
    gWsaLock.acquire()
    gTimerDeadline = 0
    gTimerHandler = nil
    gWsaLock.release()

# --- I/O polling ---

proc poolPollIo*(timeoutMs: cint): bool =
  ## Poll the shared I/O instance once and dispatch every ready handler on the
  ## calling thread. `timeoutMs` is how long to block waiting for an event (`0`
  ## = non-blocking peek). Returns true if at least one handler fired. Safe to
  ## call from any thread (oneshot semantics: each event goes to exactly one
  ## caller) — used both by the worker loop and by `parWait` so a thread blocked
  ## on a join can still advance the I/O its parked chunks are waiting on.
  result = false
  when hasEpoll:
    var ioEvents {.noinit.}: array[MaxIoEvents, EpollEvent]
    let n = epoll_wait(gIoFd, addr ioEvents[0], MaxIoEvents.cint, timeoutMs)
    for i in 0 ..< n:
      let h = cast[ptr IoHandler](ioEvents[i].data.p)
      if h != nil:
        h.cb(h, ioEvents[i].events)
    result = n > 0
  elif hasKqueue:
    var ioEvents {.noinit.}: array[MaxIoEvents, KEvent]
    var ts = default Timespec
    if timeoutMs > 0:
      ts.tv_nsec = 1_000_000  # 1 ms (we only ever pass 0 or 1)
    let n = kevent(gIoFd, nil, 0, addr ioEvents[0], MaxIoEvents.cint, addr ts)
    for i in 0 ..< n:
      let h = cast[ptr IoHandler](ioEvents[i].udata)
      if h != nil:
        let evMask = case ioEvents[i].filter
          of EVFILT_READ: EvRead
          of EVFILT_WRITE: EvWrite
          else: 0u32
        h.cb(h, evMask)
    result = n > 0
  elif hasWsaPoll:
    result = false
    # --- Deadline timer, checked BEFORE the poll gate ---
    # The timer must not be starved just because another thread holds the single-
    # poller gate, so every worker's tick gets a chance to fire it — check here,
    # ahead of the testAndSet below. Claim under the lock (take the handler local,
    # clear the deadline) and dispatch OUTSIDE the lock with `events == 0`, which
    # marks a timer expiry versus fd readiness (always non-zero). Exactly-once per
    # arming; the callback may re-arm (curl does) because no lock is held and no
    # state is touched after dispatch.
    block:
      var th: ptr IoHandler = nil
      gWsaLock.acquire()
      if gTimerDeadline != 0 and int64(getTickCount64()) >= gTimerDeadline:
        th = gTimerHandler
        gTimerDeadline = 0
        gTimerHandler = nil
      gWsaLock.release()
      if th != nil:
        th.cb(th, 0'u32)
        result = true
    # Single poller at a time: WSAPoll has no kernel set, so two threads polling
    # overlapping snapshots could both deliver the same fd. A CAS elects one
    # poller; losers just sleep out the timeout. Safe because `workerLoop` only
    # ever passes 0 or 1 ms, so a blocked WSAPoll delays nothing by more than
    # ~1 ms (no cross-thread wakeup mechanism is needed).
    if testAndSet(gWsaPolling, moAcquire):
      if timeoutMs > 0: sleep(timeoutMs.uint32)
      return result
    # Snapshot the armed entries under the lock, then poll without it held.
    var pfds {.noinit.}: array[MaxIoEvents, WSAPOLLFD]
    var slotIdx {.noinit.}: array[MaxIoEvents, int]
    var n = 0
    gWsaLock.acquire()
    for i in 0 ..< gWsaSlots.len:
      if n >= MaxIoEvents: break
      if gWsaSlots[i].handler != nil and gWsaSlots[i].events != 0:
        var ev = cshort(0)
        if (gWsaSlots[i].events and EvRead) != 0: ev = ev or POLLRDNORM
        if (gWsaSlots[i].events and EvWrite) != 0: ev = ev or POLLWRNORM
        pfds[n].fd = cast[SocketHandle](gWsaSlots[i].fd)
        pfds[n].events = ev
        pfds[n].revents = cshort(0)
        slotIdx[n] = i
        inc n
    # Table-overflow failure mode: only the first MaxIoEvents (== the 64-slot
    # table) armed fds are polled; a 65th distinct fd would be silently skipped
    # each tick. Can't happen at the current table size, but the two limits are
    # sized together for the client use-case — a Windows *server* needing more fds
    # wants the IOCP backend, not a bigger poll set.
    gWsaLock.release()
    if n == 0:
      # Nothing armed: honour the idle timeout so the worker loop doesn't spin.
      if timeoutMs > 0: sleep(timeoutMs.uint32)
      clear(gWsaPolling, moRelease)
      return result
    let ready = wsaPoll(addr pfds[0], n.culong, timeoutMs)
    if ready > 0:
      # Claim phase (under the lock) then dispatch phase (outside it). Collect the
      # fired (handler, mask) pairs and disarm the delivered bits while holding the
      # lock, release, CLEAR THE GATE, and only then run callbacks. Dispatching
      # under the gate would let a callback that raises leave `gWsaPolling` set
      # forever — every future poolPollIo would degrade to a sleep, a permanent
      # global I/O stall. Clearing first is safe: the oneshot claim already
      # disarmed these fds, so a concurrent poller entering WSAPoll mid-dispatch
      # simply won't see them.
      var firedH {.noinit.}: array[MaxIoEvents, ptr IoHandler]
      var firedMask {.noinit.}: array[MaxIoEvents, uint32]
      var m = 0
      var k = 0
      gWsaLock.acquire()
      while k < n:
        let re = int(pfds[k].revents)
        if re != 0:
          let i = slotIdx[k]
          # Re-validate: the slot must still hold the same fd we polled — it may
          # have been unregistered after we dropped the lock for the poll.
          #
          # fd/slot-reuse caveat: while the lock was dropped, another thread can
          # unregister fd N, the OS can reuse that handle value for a brand-new
          # socket, and re-register it here with a DIFFERENT handler. fd equality
          # then still passes and we deliver the OLD socket's readiness to the NEW
          # handler. This is NOT a use-after-free — the handler is re-read live
          # under the lock; the only consequence is a spurious wakeup (the consumer
          # attempts I/O, sees WOULDBLOCK, re-arms). The proper fix is the planned
          # fdslots/backend redesign — see hashi doc/iocp-ioring-briefing.md.
          if gWsaSlots[i].handler != nil and
             cast[SocketHandle](gWsaSlots[i].fd) == pfds[k].fd:
            var mask: uint32 = 0
            if (re and PollErrMask) != 0:
              # Error/hang-up/invalid: deliver the full armed mask so the
              # consumer attempts I/O and observes the socket error — matching
              # epoll's EPOLLERR wakeup behaviour.
              mask = gWsaSlots[i].events
            else:
              if (re and int(POLLRDNORM)) != 0: mask = mask or EvRead
              if (re and int(POLLWRNORM)) != 0: mask = mask or EvWrite
            # Oneshot claim: clear the delivered bits under the lock so the fd
            # stays disarmed until rearmFd — exactly one caller ever sees a given
            # fired event.
            mask = mask and gWsaSlots[i].events
            if mask != 0:
              gWsaSlots[i].events = gWsaSlots[i].events and not mask
              firedH[m] = gWsaSlots[i].handler
              firedMask[m] = mask
              inc m
        inc k
      gWsaLock.release()
      clear(gWsaPolling, moRelease)
      # Dispatch phase: gate already cleared, no lock held — a raising callback
      # can't poison the poller, and a callback that re-arms its own fd works.
      var j = 0
      while j < m:
        let h = firedH[j]
        if h != nil:
          h.cb(h, firedMask[j])
          result = true
        inc j
    else:
      clear(gWsaPolling, moRelease)
  else:
    if timeoutMs > 0:
      discard usleepMicroseconds(timeoutMs.uint32 * 1000'u32)

# --- Worker loop ---

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

proc workerLoop(arg: pointer) {.nimcall.} =
  let threadIdx = cast[int](arg)   # index passed by value via the pointer slot (see initPool)
  var idleTicks = 0
  var sinceCollect = 0
  while not atomicLoad(stopFlag, moRelaxed):
    # 1. Bulk-drain tasks: own stripe first, then steal from others. Trampolines
    #    each continuation, re-submitting any that yield more work.
    let busy = drainOnce(threadIdx)
    # 2. Poll I/O — non-blocking when we just ran work, 1ms wait when idle.
    discard poolPollIo(if busy: 0.cint else: 1.cint)
    # 3. Reclaim this worker's cross-thread-free backlog: a forced collect
    #    after a brief idle (~8ms of 1ms polls), plus a hard periodic fallback
    #    so a worker that never goes idle still collects. force=true is what
    #    actually drains the remote list; at this cadence its cost is noise.
    #    (mimalloc-only: the default nimNativeAlloc has its own cross-thread
    #    free path — measure before assuming it needs an equivalent.)
    when defined(useMimalloc):
      inc sinceCollect
      if busy:
        idleTicks = 0
      else:
        inc idleTicks
      if idleTicks >= 8 or sinceCollect >= 8192:
        idleTicks = 0
        sinceCollect = 0
        miCollect(true)

# --- Lifecycle ---

proc initPool*() =
  ## Initialize the I/O poller and start worker threads.
  when hasEpoll:
    gIoFd = epoll_create1(0)
    assert gIoFd >= 0, "epoll_create1 failed"
  elif hasKqueue:
    gIoFd = kqueue()
    assert gIoFd >= 0, "kqueue failed"
  elif hasWsaPoll:
    # Bring Winsock up (2.2). curl also raises the refcount via CURL_GLOBAL_WIN32;
    # WSAStartup is refcounted, so both starting it is fine. WSADATA is opaque to
    # us — a byte buffer large enough for its layout is all the API needs.
    var wsaData {.noinit.}: array[512, byte]
    let rc = wsaStartup(0x0202'u16, addr wsaData[0])
    assert rc == 0, "WSAStartup failed"
  for i in 0 ..< WorkerCount:
    try:
      # Pass the worker index BY VALUE through the pointer slot. It used to be
      # `addr indexes[i]` into a stack-local array that dangled the moment
      # initPool returned — run()'s sleep frame then reused that memory and the
      # workers read garbage thread indices (valgrind: uninitialised value in
      # tryBulkDequeue/workerLoop, origin the reused frame). No storage, no
      # lifetime, no race.
      create workers[i], workerLoop, cast[pointer](i)
    except:
      discard

proc poolStopped*(): bool {.inline.} =
  atomicLoad(stopFlag, moRelaxed)

proc shutdownPool*() =
  ## Signal all workers to stop and join threads.
  atomicStore(stopFlag, true, moRelaxed)
  for i in 0 ..< WorkerCount:
    workers[i].join()
  when hasEpoll or hasKqueue:
    proc close(fd: cint): cint {.importc, header: "<unistd.h>".}
    discard close(gIoFd)
  # No WSACleanup on Windows: curl and app code share the process-wide Winsock
  # refcount, so churning it on pool shutdown isn't worth the teardown races.
