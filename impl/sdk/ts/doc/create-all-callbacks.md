# Create all the callbacks
If you are coming to this doc it is probably because you have thought that it
is possible and more elegant to not create all the callbacks returned
from a suspended computation/coroutine (who knows what else we are going to
call it in the future). And as many people before you we all thought it was
elegant and nice and that it had no problems.

Well you are wrong, just do what the title says and be happy, or read the rest
of this small doc to understand why.

You might be thinking of this kind of code:

```
def root:
	p1 = yield rfi("a")
	p2 = yield rfi("b")
	p3 = yield rfi("c")
	v3 = yield p3 // We only need to create this callback
```

It is true that in this kind of code, creating all the callbacks might be not
the best idea. What would happen if the callbacks for p1 or p2 arrive before
the one for p3, we will try to resume a coroutine that cannot move forward
and even try to create some more callbacks unnecessarily. Thankfully, the
server knows that it can only create one callback and dedups, we have stable,
deterministic ids for callbacks, so trying to create the same callback several
times is not really problematic from the correctness point of view.

However, from the efficiency point of view, we are being wasteful, we are
hitting the server more than it is necessary, we are being "noisy".

Consider the following code:
```
def foo:
	// something super quick

def bar:
	time.sleep(1year) // something super slow

def a:
	p1 = yield rfi("foo")
	v1 = yield p1
	c = v1 + 2
	d = c + 20
	// some more code

def b:
	p2 = yield rfi("bar")
	v2 = yield p2
	// some more important stuff to do

def root:
	f1 = yield lfi(a)
	f2 = yield lfi(b)
	v = yield f2 // we will block on a nested rfi here
```

That code is problematic if we only create a callback for the "bar" rfi.
It would be possible to somehow keep track of which futures are blocked by
which rfis and find which is the callback that needs to be created, that
is not the problem here.
The problem is that we will not resume the root coroutine, hence we will
not resume the "a" coroutine, until bar finishes, and bar will take 1 year
to complete.
Instead if we keep track of all the rfis, including the nested ones, and create
all the callbacks when we suspend the execution of the root coroutine, we
will be able to make progress on "a" even though b has not completed.

That is why we need to create all the callbacks.

Thanks for coming to my TED talk and create all the callbacks.
