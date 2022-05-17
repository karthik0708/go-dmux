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
	failureRateTh 	float64
	retryFactor 	float64
	timeout       	time.Duration
	lock            sync.Mutex
	state           uint32
	queue 			Queue
}

type Queue struct{
	slice []uint32
	cap int
	len int
}

func New(failureRateTh float64, timeout time.Duration, windowSize int, retryFactor float64) *Breaker {
	var slice = make([]uint32, 0, windowSize)
	return &Breaker{
		failureRateTh:   failureRateTh,
		timeout:          timeout,
		queue: 			Queue{slice: slice, cap: windowSize, len: 0},
		retryFactor: retryFactor,
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
	//send error or success signal to monitor accordingly
	if result == nil && panicValue == nil {
		//log.Println("worker.goroutine: sending signal", Success)
		monitorCh <- Success
	} else {
		monitorCh <- Error
		//log.Println("worker.goroutine: sending signal", Error)
	}
}

func (b *Breaker) MonitorBreaker(size int, workerChs []chan uint32, monitorCh <- chan uint32){
	//To indicate current number of not Processed signals
	cnt := 0

	//Indicates the number of workers to Resume when changing from a half open state
	chosenCnt := int(math.Ceil(b.retryFactor*float64(size)))

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

			//Send Resume signals twice the number of already chosen workers if none of those are processing events
			if cnt == chosenCnt{
				chosenCnt = int(math.Min(float64(size), float64(2*chosenCnt)))
				cnt = 0
				sendSignal(Play, chosenCnt, workerChs)
			}

		case Error, Success:
			//log.Println("monitor.goroutine: recieved signal", signal)

			//populate the queue until its full post which new signals are enqueued and older ones dequeued
			if b.queue.len < b.queue.cap {
				b.queue.enqueue(signal)
				if b.queue.len != b.queue.cap {
					sendSignal(Play, size, workerChs)
					continue
				} else{
					failureRate = b.computeRate()
				}
			} else {
				cnt = 0
				chosenCnt = int(math.Ceil(b.retryFactor * float64(size)))
				b.queue.dequeue()
				b.queue.enqueue(signal)
				failureRate = b.computeRate()
			}
			fmt.Println(failureRate)

			//If current failure rate exceeds threshold then open the breaker
			if failureRate >= b.failureRateTh {
				if b.state == Closed || b.state == HalfOpen{
					b.openBreaker(workerChs, size, chosenCnt)
				}
			} else {
				if b.state == HalfOpen {
					b.closeBreaker(workerChs, size)
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

func (b *Breaker) openBreaker(workerChs []chan uint32, size int, chosenCnt int) {
	b.changeState(Open)
	sendSignal(Pause, size, workerChs)
	//start a timer once timeout duration is up resume selected workers
	go b.timer(workerChs, chosenCnt)
}

func (b *Breaker) closeBreaker(workerChs []chan uint32, size int) {
	b.changeState(Closed)
	sendSignal(Play, size, workerChs)
}

func (b *Breaker) timer(workerChs []chan uint32, chosenCnt int) {
	time.Sleep(b.timeout)

	b.lock.Lock()
	defer b.lock.Unlock()

	b.changeState(HalfOpen)
	//log.Printf("monitor.goroutine: sending signal %v to %v goroutines\n", Play, chosenCnt)
	sendSignal(Play, chosenCnt, workerChs)
}

func (b *Breaker) changeState(newState uint32) {

	atomic.StoreUint32(&b.state, newState)

	log.Println("status of circuit is ", newState)
}

func sendSignal(signal uint32, chosenCnt int, workerChs []chan uint32) {
	for i := 0; i < chosenCnt; i++ {
		workerChs[i] <- signal
	}
}

func (q *Queue)enqueue(element uint32){
	q.slice = append(q.slice, element) // Simply append to enqueue.
	q.len++
}

func (q *Queue)dequeue() {
	q.slice = q.slice[1:]
	q.len--
}
