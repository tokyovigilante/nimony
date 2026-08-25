# Repro: an early `return` inside a while loop of a passive proc is clobbered
# by the post-loop statements when an `if` branch containing a suspension,
# followed by an `elif` arm with an early `return`, precedes the loop.
#
# The lowering wraps every trailing statement INSIDE the loop in the usual
# `if (!g)` / `if (!r)` early-return guards, but emits the POST-loop block
# guarded only by the loop-exit flag — so the loop's `return 2` runs, sets
# the result, and is then overwritten by the unguarded `return 3` path.
# In larger procs the post-loop scope cleanup also re-destroys frame slots
# the early-return path already destroyed (double nimStrDestroy / double
# refcount dec → heap corruption under load).
#
# All four elements are required: the suspension in the `if` arm, the
# `return` in the `elif` arm (a separate plain `if` lowers correctly), the
# while loop with an early return, and post-loop statements.
#
# Expected output: 2   (currently prints 3)
#
# Related: with `result = pass()` as the tail (instead of `result = 4`), or
# with the loop's `discard pass()` removed, hexer dies instead with
# "[Bug] unexpected ')' inside".
import std / syncio

var gMode = 2

proc pass(): int {.passive.} =
  result = 1

proc ensure(): int {.passive.} =
  if gMode == 1:
    discard pass()      # suspension inside the if branch
  elif gMode == 3:
    return 1            # early return in the elif
  var claimed = 1
  var polls = 0
  while claimed == 1 and polls < 3:
    discard pass()
    polls = polls + 1
    if polls == 1:
      return 2          # early return in the loop — must win
    claimed = 0
  if claimed == 1:
    return 3            # must be skipped; the miscompile lands here
  result = 4

echo ensure()
