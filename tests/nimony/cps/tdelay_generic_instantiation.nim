# `delay` inside a GENERIC .passive proc.
#
# semDelay rewrites `delay(call)` into the flat `(delay fn args)`. Generic
# instantiation re-sems a body the first pass already flattened, so semDelay
# sees its own output and must accept it: on the second visit the child is the
# stripped `fn`, not a call. Without that, instantiating this proc fails with
# "`delay` takes a call expression or no argument".
import std/syncio

proc worker(v: int) {.passive.} =
  echo "worked"

proc runner[T](v: T) {.passive.} =
  echo "before delay"
  discard delay worker(1)
  echo "after delay"

proc main() {.passive.} =
  runner(42)

main()
