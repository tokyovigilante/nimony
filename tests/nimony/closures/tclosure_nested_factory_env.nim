## A NESTED routine that returns a closure over its own parameter or local
## must give every call an environment of its own. Lambda lifting kept one
## environment per lifting root (`main`) and hoisted `make`'s captured
## parameter into it, so every closure `make` returned shared one `e`:
## `make(0)()` answered with the value of the last call.
import std/syncio
import deps/mclosureenvleak

type Hook = proc(): int {.closure.}

proc paramFactory() =
  proc make(e: int): Hook {.closure.} =
    result = proc(): int {.closure.} = e
  let h0 = make(0)
  let h1 = make(10)
  echo h0(), " ", h1()

proc localFactory() =
  proc make(e: int): Hook =
    let x = e * 2
    result = proc(): int {.closure.} = x
  let h0 = make(1)
  let h1 = make(2)
  let h2 = make(3)
  echo h0(), " ", h1(), " ", h2()

proc mixedFactory() =
  # the closure captures `base` of the enclosing proc AND `make`'s parameter
  var base = 100
  proc make(e: int): Hook =
    result = proc(): int {.closure.} = base + e
  let h0 = make(0)
  let h1 = make(10)
  base = 200
  echo h0(), " ", h1()

proc counterFactory() =
  # each counter mutates its own captured state; `total` is shared
  var total = 0
  proc counter(start: int): Hook =
    var n = start
    result = proc(): int {.closure.} =
      inc n
      inc total
      n
  let a = counter(0)
  let b = counter(100)
  discard a()
  discard a()
  discard b()
  echo a(), " ", b(), " ", total

proc deepFactory() =
  # a factory nested in a factory: each level's state per call
  proc outer(a: int): Hook =
    proc inner(b: int): Hook =
      result = proc(): int {.closure.} = a * 10 + b
    result = inner(a + 1)
  let g1 = outer(1)
  let g2 = outer(2)
  let g3 = outer(3)
  echo g1(), " ", g2(), " ", g3()

proc callerOwnsLocal() =
  # a closure called from a sibling that has a captured local of its own:
  # the sibling must hand on the enclosing proc's environment, not its own
  var k = 7
  proc isK(n: int): bool {.closure.} = n == k
  proc probe(n: int): Hook =
    let m = n
    echo isK(m)
    result = proc(): int {.closure.} = m
  let p = probe(7)
  let q = probe(8)
  echo p(), " ", q()

proc stackChain() =
  # nothing escapes, so the environments live on the stack; `mid`'s still
  # links to `stackChain`'s
  var base = 100
  proc mid(e: int): int =
    proc inner(): int {.closure.} = base + e
    result = inner()
  echo mid(1), " ", mid(2)

proc iterFactory() =
  # a closure iterator nested in a factory, over the factory's parameter
  var step = 1
  proc make(top: int): iterator(): int {.closure.} =
    result = iterator(): int {.closure.} =
      var i = 0
      while i < top:
        yield i
        i += step
  let a = make(2)
  let b = make(3)
  var s = ""
  for x in a(): s.add $x
  s.add "|"
  for x in b(): s.add $x
  echo s

proc chainedRelease() =
  # a chained environment releases its own captures when its closure goes,
  # and keeps the enclosing one alive only as long as it needs it
  var outerRes = Resource(name: "outer")
  proc make(tag: string): Hook =
    var innerRes = Resource(name: tag)
    result = proc(): int {.closure.} =
      echo outerRes.name, " ", innerRes.name
      result = 1
  block:
    let h = make("inner")
    discard h()
  echo "dropped"

paramFactory()
localFactory()
mixedFactory()
counterFactory()
deepFactory()
callerOwnsLocal()
stackChain()
iterFactory()
chainedRelease()
echo "ok"
