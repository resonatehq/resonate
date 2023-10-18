package scheduler

type Coroutine[I, O any] struct {
	name string
	f    func(*Coroutine[I, O])
	c_i  chan *WrapI[I]
	c_o  chan *WrapO[O]

	// lifecycle
	onDone []func()

	// scheduler
	Scheduler S
}

type WrapI[I any] struct {
	Value I
	Error error
}

type WrapO[O any] struct {
	Value O
	Done  bool
}

func NewCoroutine[I, O any](name string, f func(*Coroutine[I, O])) *Coroutine[I, O] {
	c := &Coroutine[I, O]{
		f:    f,
		c_i:  make(chan *WrapI[I]),
		c_o:  make(chan *WrapO[O]),
		name: name,
	}

	go func() {
		<-c.c_i
		c.f(c)
		close(c.c_i)

		c.c_o <- &WrapO[O]{Done: true}
		close(c.c_o)
	}()

	return c
}

func (c *Coroutine[I, O]) OnDone(f func()) {
	c.onDone = append(c.onDone, f)
}

func (c *Coroutine[I, O]) Yield(o O) (I, error) {
	c.c_o <- &WrapO[O]{Value: o, Done: false}
	i := <-c.c_i

	return i.Value, i.Error
}

func (c *Coroutine[I, O]) Resume(i I, e error) *WrapO[O] {
	c.c_i <- &WrapI[I]{Value: i, Error: e}
	return <-c.c_o
}

func (c *Coroutine[I, O]) Time() int64 {
	return c.Scheduler.Time()
}
