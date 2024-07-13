package scheduler

// import (
// 	"fmt"

// 	"github.com/resonatehq/resonate/internal/kernel/metadata"
// )

// type Coroutine[I, O any] struct {
// 	f   func(*Coroutine[I, O])
// 	c_i chan *WrapI[I]
// 	c_o chan *WrapO[O]

// 	// lifecycle
// 	onDone []func()

// 	// metadata
// 	metadata *metadata.Metadata

// 	// scheduler
// 	Scheduler S
// }

// func (c *Coroutine[I, O]) String() string {
// 	return fmt.Sprintf("Coroutine(metadata=%s)", c.metadata)
// }

// type WrapI[I any] struct {
// 	Value I
// 	Error error
// }

// type WrapO[O any] struct {
// 	Value O
// 	Done  bool
// }

// func NewCoroutine[I, O any](metadata *metadata.Metadata, f func(*Coroutine[I, O])) *Coroutine[I, O] {
// 	c := &Coroutine[I, O]{
// 		f:        f,
// 		c_i:      make(chan *WrapI[I]),
// 		c_o:      make(chan *WrapO[O]),
// 		metadata: metadata,
// 	}

// 	go func() {
// 		<-c.c_i
// 		c.f(c)
// 		close(c.c_i)

// 		c.c_o <- &WrapO[O]{Done: true}
// 		close(c.c_o)
// 	}()

// 	return c
// }

// func (c *Coroutine[I, O]) OnDone(f func()) {
// 	c.onDone = append(c.onDone, f)
// }

// func (c *Coroutine[I, O]) Yield(o O) (I, error) {
// 	c.c_o <- &WrapO[O]{Value: o, Done: false}

// 	i := <-c.c_i
// 	return i.Value, i.Error
// }

// func (c *Coroutine[I, O]) Resume(i I, e error) (O, bool) {
// 	c.c_i <- &WrapI[I]{Value: i, Error: e}

// 	o := <-c.c_o
// 	return o.Value, o.Done
// }

// func (c *Coroutine[I, O]) Time() int64 {
// 	return c.Scheduler.Time()
// }
