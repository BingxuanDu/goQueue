package queue

import (
    "fmt"
    "sync"
    "sync/atomic"
)

type Queue struct {
    capacity uint32
    getPos   uint32
    putPos   uint32
    size     uint32
    cond     *sync.Cond
    caches   []cache
}

type cache struct {
    value interface{}
}

// CreateQueue creates and returns a Queue instance
func CreateQueue(capacity uint32) *Queue {
    q := new(Queue)
    q.capacity = capacity
    q.size = 0
    q.getPos = 0
    q.putPos = 0
    q.caches = make([]cache, capacity)
    q.cond = sync.NewCond(&sync.Mutex{})
    return q
}

// Capacity returns the capacity of the queue
func (q *Queue) Capacity() uint32 {
    return q.capacity
}

// Size returns queue's current size
func (q *Queue) Size() uint32 {
    size := atomic.LoadUint32(&q.size)
    return size
}

// String method formats queue's output
func (q *Queue) String() string {
    return fmt.Sprintf("Queue{capaciity: %v, size: %v, getPos: %v, putPos: %v, caches: %v}",
        q.capacity, q.size, q.getPos, q.putPos, q.caches)
}

// Put an element into queue.
// The whole process supported by CAS to assure atomicity, without block.
// And the CAS way is much faster than Lock way.
// When current size >= capacity, returns error.
func (q *Queue) Put(value interface{}) (bool, error) {
    size := atomic.LoadUint32(&q.size)
    putPos := atomic.LoadUint32(&q.putPos)
    if size >= q.capacity {
        return false, fmt.Errorf("queue is full")
    }

    putPosNew := (putPos + 1) % q.capacity
    if !atomic.CompareAndSwapUint32(&q.putPos, putPos, putPosNew) ||
        !atomic.CompareAndSwapUint32(&q.size, size, size+1) {
        return false, fmt.Errorf("the queue has been changed, sync failed")
    }

    cache := &q.caches[putPos]
    cache.value = value
    return true, nil
}

// PutWithBlock method put an element into queue with block.
// When current size >= the capacity, it'll be waiting until consumer consumes.
func (q *Queue) PutWithBlock(value interface{}) {
    q.cond.L.Lock()
    defer q.cond.L.Unlock()
    if q.size >= q.capacity {
        q.cond.Wait()
    }

    putPosNew := (q.putPos + 1) % q.capacity
    cache := &q.caches[q.putPos]
    cache.value = value
    q.putPos = putPosNew
    q.size++
    q.cond.Broadcast()
}

// Get with no block, same as Put().
func (q *Queue) Get() (interface{}, error) {
    size := atomic.LoadUint32(&q.size)
    getPos := atomic.LoadUint32(&q.getPos)
    if size <= 0 {
        return nil, fmt.Errorf("queue is empty")
    }

    getPosNew := (getPos + 1) % q.capacity
    if !atomic.CompareAndSwapUint32(&q.getPos, getPos, getPosNew) ||
        !atomic.CompareAndSwapUint32(&q.size, size, size-1) {
        return false, fmt.Errorf("the queue has been changed, sync failed")
    }

    cache := &q.caches[getPos]
    val := cache.value
    cache.value = nil
    return val, nil
}

// GetWithBlock get element with block, same as PutWithBlock().
func (q *Queue) GetWithBlock() interface{} {
    q.cond.L.Lock()
    defer q.cond.L.Unlock()
    if q.size <= 0 {
        q.cond.Wait()
    }

    getPosNew := (q.getPos + 1) % q.capacity
    cache := &q.caches[q.getPos]
    val := cache.value
    cache.value = nil
    q.getPos = getPosNew
    size := q.size
    q.size--

    if size >= q.capacity {
        q.cond.Broadcast()
    }
    return val
}
