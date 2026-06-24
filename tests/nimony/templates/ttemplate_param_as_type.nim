# Regression guard: a generic routine called inside a template body, with a
# template parameter used as the generic type argument (`GVec2[typ]` /
# `gvec2[typ]`), used to CRASH the compiler during overload resolution —
# `typeprops.typeImpl` asserted `result.stmtKind == TypeS`, but the formal
# symbol resolved to the template parameter `typ` (symKind ParamY), not a type.
# `matchSymbol` now handles the non-type placeholder gracefully instead of
# asserting. (This pattern is vmath's genVecConstructor. Making it *compile*
# rather than error needs the eager template-body type-check to defer to
# instantiation — tracked separately; this test only pins that it no longer
# crashes the compiler.)
type GVec2[T] = array[2, T]
proc gvec2[T](x, y: T): GVec2[T] = [x, y]
template genCtor(typ: untyped) =
  proc mk(x, y: typ): GVec2[typ] = gvec2[typ](x, y)
genCtor(float32)
