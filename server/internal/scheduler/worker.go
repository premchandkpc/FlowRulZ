package scheduler

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

type Timer struct {
	ID       uint64
	Callback func()
}

type TimerWheel struct {
	mu          sync.Mutex
	tick        time.Duration
	slotCount   int
	slots       []*list.List
	currentSlot int
	nextID      atomic.Uint64
	ticker      *time.Ticker
	done        chan struct{}
	stopOnce    sync.Once
	entries     map[uint64]*list.Element
	wg          sync.WaitGroup
}

type timerEntry struct {
	id        uint64
	remaining int
	callback  func()
	slotIdx   int
}

func NewTimerWheel(tick time.Duration, slotCount int) *TimerWheel {
	slots := make([]*list.List, slotCount)
	for i := range slots {
		slots[i] = list.New()
	}
	return &TimerWheel{
		tick:      tick,
		slotCount: slotCount,
		slots:     slots,
		done:      make(chan struct{}),
		entries:   make(map[uint64]*list.Element),
	}
}

func (tw *TimerWheel) Start() {
	tw.ticker = time.NewTicker(tw.tick)
	go tw.run()
}

func (tw *TimerWheel) Stop() {
	tw.stopOnce.Do(func() {
		if tw.ticker != nil {
			tw.ticker.Stop()
		}
		close(tw.done)
	})
	tw.wg.Wait()
}

func (tw *TimerWheel) Add(d time.Duration, callback func()) *Timer {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	id := tw.nextID.Add(1)
	ticks := int(d / tw.tick)
	if ticks < 1 {
		ticks = 1
	}
	slot := (tw.currentSlot + ticks) % tw.slotCount
	remaining := ticks / tw.slotCount

	entry := &timerEntry{
		id:        id,
		remaining: remaining,
		callback:  callback,
		slotIdx:   slot,
	}
	elem := tw.slots[slot].PushBack(entry)
	tw.entries[id] = elem

	return &Timer{ID: id, Callback: callback}
}

func (tw *TimerWheel) Cancel(id uint64) bool {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	elem, ok := tw.entries[id]
	if !ok {
		return false
	}
	entry := elem.Value.(*timerEntry)
	tw.slots[entry.slotIdx].Remove(elem)
	delete(tw.entries, id)
	return true
}

func (tw *TimerWheel) Len() int {
	tw.mu.Lock()
	defer tw.mu.Unlock()
	return len(tw.entries)
}

func (tw *TimerWheel) run() {
	for {
		select {
		case <-tw.ticker.C:
			tw.tickOnce()
		case <-tw.done:
			return
		}
	}
}

func (tw *TimerWheel) tickOnce() {
	tw.mu.Lock()

	var fire []func()
	slot := tw.slots[tw.currentSlot]
	next := slot.Front()
	for next != nil {
		entry := next.Value.(*timerEntry)
		if entry.remaining > 0 {
			entry.remaining--
			targetSlot := (tw.currentSlot + 1) % tw.slotCount
			entry.slotIdx = targetSlot
			elem := tw.slots[targetSlot].PushBack(entry)
			tw.entries[entry.id] = elem
		} else {
			fire = append(fire, entry.callback)
		}
		tmp := next
		next = next.Next()
		slot.Remove(tmp)
		delete(tw.entries, entry.id)
	}

	tw.currentSlot = (tw.currentSlot + 1) % tw.slotCount
	tw.mu.Unlock()

	for _, cb := range fire {
		tw.wg.Add(1)
		go func(fn func()) {
			defer tw.wg.Done()
			fn()
		}(cb)
	}
}
