package distributor

import (
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap/zaptest"
	"sync"
	"testing"
	"time"
)

func TestDistributorSimple(t *testing.T) {
	d := NewDistributor(zaptest.NewLogger(t))
	defer d.Close()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ch, unsub := d.Subscribe(0)
		defer unsub(true)
		data, ok := <-ch
		assert.Equal(t, float64(12), data.A)
		assert.Equal(t, uint64(1), data.ID)
		assert.True(t, ok)
	}()
	time.Sleep(1)
	d.Distribute(Data{A: 12})
	wg.Wait()
}

func TestDistributorReAssign(t *testing.T) {
	d := NewDistributor(zaptest.NewLogger(t))
	defer d.Close()

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		ch, unsub := d.Subscribe(0)
		defer unsub(false) // <- should cause re-assign
		data := <-ch
		assert.Equal(t, float64(12), data.A)
		assert.Equal(t, uint64(1), data.ID)
	}()
	time.Sleep(time.Second)
	go func() {
		defer wg.Done()
		ch, unsub := d.Subscribe(0)
		defer unsub(true)
		data := <-ch
		assert.Equal(t, float64(12), data.A)
		assert.Equal(t, uint64(1), data.ID)
	}()

	time.Sleep(time.Second)
	d.Distribute(Data{A: 12})
	wg.Wait()
}

func TestDistributorOutOfDate(t *testing.T) {
	d := NewDistributor(zaptest.NewLogger(t))
	defer d.Close()

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		defer wg.Done()
		ch, unsub := d.Subscribe(0)
		defer unsub(false) // <- should cause re-assign
		data := <-ch
		assert.Equal(t, float64(12), data.A)
		assert.Equal(t, uint64(1), data.ID)
	}()

	time.Sleep(time.Second)
	d.Distribute(Data{A: 12})

	go func() {
		defer wg.Done()
		ch, unsub := d.Subscribe(0)
		defer unsub(true)
		data := <-ch
		assert.Equal(t, float64(12), data.A)
		assert.Equal(t, uint64(1), data.ID)
	}()

	wg.Wait()
}

func TestDistributorOrder(t *testing.T) {
	d := NewDistributor(zaptest.NewLogger(t))
	defer d.Close()

	wg := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		id := i
		go func() {
			defer wg.Done()
			ch, unsub := d.Subscribe(0)
			defer unsub(true) // <- should cause re-assign
			data, ok := <-ch
			assert.Equal(t, float64(id), data.A)
			assert.Equal(t, uint64(id+1), data.ID)
			assert.True(t, ok)
		}()
		time.Sleep(time.Millisecond * 100)
	}

	for i := 0; i < 10; i++ {
		d.Distribute(Data{A: float64(i)})
	}

	wg.Wait()
}

func TestDistributorOrderWithReassign(t *testing.T) {
	d := NewDistributor(zaptest.NewLogger(t))
	defer d.Close()

	wg := sync.WaitGroup{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		id := i
		expId := int(i / 2)
		go func() {
			defer wg.Done()
			ch, unsub := d.Subscribe(0)
			defer unsub(id%2 != 0) // <- should cause re-assign
			data, ok := <-ch
			assert.Equal(t, float64(expId), data.A)
			assert.Equal(t, uint64(expId+1), data.ID)
			assert.True(t, ok)
		}()
		time.Sleep(time.Millisecond * 100)
	}

	for i := 0; i < 5; i++ {
		d.Distribute(Data{A: float64(i)})
		time.Sleep(time.Millisecond * 100)
	}

	wg.Wait()
}

func TestDistributor_Close(t *testing.T) {
	d := NewDistributor(zaptest.NewLogger(t))

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ch, unsub := d.Subscribe(0)
		defer unsub(false) // <- should cause re-assign
		_, ok := <-ch
		assert.False(t, ok)
	}()

	time.Sleep(time.Second)

	d.Close()

	wg.Wait()
}

func TestDistributor_Close2(t *testing.T) {
	d := NewDistributor(zaptest.NewLogger(t))
	d.Close()

	d.Distribute(Data{}) // should not cause problem
	d.Distribute(Data{}) // should not cause problem
}
