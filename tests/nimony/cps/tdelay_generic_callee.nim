# `delay` of a GENERIC callee, from inside a generic .passive proc.
#
# Instantiation re-sem hands semDelay the raw generic `(delay fn args)` and never
# re-sems the delayed callee, so the callee stays uninstantiated: no .coro frame
# is emitted and the backend asserts. Distinct from the idempotence fix — this
# one needs the CALLEE instantiated, not just the delay shape accepted.
import std/syncio

proc worker[T](v: T) {.passive.} =
  echo "worked"

proc runner[T](v: T) {.passive.} =
  echo "before delay"
  discard delay worker(v)
  echo "after delay"

proc main() {.passive.} =
  runner(42)

main()
