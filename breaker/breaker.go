package breaker

import (
	"fmt"
	"log"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Closed uint32 = iota
	Open
	HalfOpen
	Error
	Success
	NotProcessed
	Pause
	Play
)

type Breaker struct {
	failureRateTh float64
	timeout       time.Duration

	lock              sync.Mutex
	state             uint32
	failureRate 	float64
	lastError         time.Time
	queue Queue
}

type Queue struct{
	slice []uint32
	cap int
	len int
}

func New(failureRateTh float64, timeout time.Duration, windowSize int) *Breaker {
	var slice = make([]uint32, 0, windowSize)
	return &Breaker{
		failureRateTh:   failureRateTh,
		timeout:          timeout,
		queue: 			Queue{slice: slice, cap: windowSize, len: 0},
	}
}

func (b *Breaker) RunWorker(work func() error, monitorCh chan<- uint32) error {
	var panicValue interface{}

	result := func() error {
		defer func() {
			panicValue = recover()
		}()
		return work()
	}()

	b.processResult(result, panicValue, monitorCh)

	if panicValue != nil {
		panic(panicValue)
	}

	return result
}

func (b *Breaker) processResult(result error, panicValue interface{}, monitorCh chan<- uint32) {

	if result == nil && panicValue == nil {
		log.Println("worker.goroutine: sending signal", Success)
		monitorCh <- Success
	} else {
		monitorCh <- Error
		log.Println("worker.goroutine: sending signal", Error)
	}
}

func (b *Breaker) MonitorBreaker(size int, breakerChs []chan uint32, factor float64, monitorCh <- chan uint32){
	cnt := 0
	chosenCnt := int(math.Ceil(factor*float64(size)))
 	var failureRate float64

	for{
		signal := <-monitorCh
		switch signal{
		case NotProcessed:

			if b.state == Closed{
				continue
			}

			cnt++

			if cnt != chosenCnt {
				continue
			}

			if cnt == chosenCnt{
				chosenCnt = int(math.Min(float64(size), float64(2*chosenCnt)))
				cnt = 0
				//log.Printf("monitor.goroutine: sending signal %v to %v goroutines\n", Play, chosenCnt)
				sendSignal(Play, chosenCnt, breakerChs)
			}

		case Error, Success:
			log.Println("monitor.goroutine: recieved signal", signal)
			if b.queue.len < b.queue.cap {
				b.queue.enqueue(signal)
				if b.queue.len != b.queue.cap {
					sendSignal(Play, size, breakerChs)
					continue
				} else{
					failureRate = b.computeRate()
				}
			} else {
				cnt = 0
				chosenCnt = int(math.Ceil(factor * float64(size)))
				b.queue.dequeue()
				b.queue.enqueue(signal)
				failureRate = b.computeRate()
			}
			fmt.Println(failureRate)
			if failureRate >= b.failureRateTh {
				if b.state == Closed || b.state == HalfOpen{
					b.openBreaker(breakerChs, size, chosenCnt)
				}
			} else {
				if b.state == HalfOpen {
					b.closeBreaker(breakerChs, size)
				}
			}
		}
	}
}

func (b *Breaker) computeRate() float64 {
	errorCnt := 0
	for _, s := range b.queue.slice{
		if Error == s {
			errorCnt++
		}
	}
	return float64(errorCnt) / float64(b.queue.len)
}

func (b *Breaker) openBreaker(breakerChs []chan uint32, size int, chosenCnt int) {
	b.changeState(Open)
	sendSignal(Pause, size, breakerChs)
	go b.timer(breakerChs, chosenCnt)
}

func (b *Breaker) closeBreaker(breakerChs []chan uint32, size int) {
	b.changeState(Closed)
	sendSignal(Play, size, breakerChs)
}

func (b *Breaker) timer(breakerChs []chan uint32, chosenCnt int) {
	time.Sleep(b.timeout)

	b.lock.Lock()
	defer b.lock.Unlock()

	b.changeState(HalfOpen)
	log.Printf("monitor.goroutine: sending signal %v to %v goroutines\n", Play, chosenCnt)
	sendSignal(Play, chosenCnt, breakerChs)
}

func (b *Breaker) changeState(newState uint32) {

	atomic.StoreUint32(&b.state, newState)

	log.Println("monitor.goroutine: status of breaker is ", newState)
}

func sendSignal(signal uint32, chosenCnt int, breakerChs []chan uint32) {
	for i := 0; i < chosenCnt; i++ {
		breakerChs[i] <- signal
	}
}

func (q *Queue)enqueue(element uint32){
	q.slice = append(q.slice, element) // Simply append to enqueue.
	q.len++
	fmt.Println("Enqueued:", element)
}

func (q *Queue)dequeue() {
	q.slice = q.slice[1:]
	q.len--
	fmt.Println("Dequeued")
}
