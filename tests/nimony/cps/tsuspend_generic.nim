# `suspend()` inside a GENERIC .passive proc.
#
# First-pass sem types the SuspendX magic as void. Generic-instantiation re-sem
# dispatches it through semSuspend, which typed it `Continuation` — so the
# discard check then rejected every generic .passive proc that suspends with
# "expression of type `Continuation` must be discarded".
import std/syncio

proc waiter[T](value: T) {.passive.} =
  echo "waiting"
  suspend()
  echo "resumed"

proc main() {.passive.} =
  waiter(42)

main()
