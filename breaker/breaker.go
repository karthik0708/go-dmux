package breaker

import (
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
	errorThreshold, successThreshold int
	timeout                          time.Duration

	lock              sync.Mutex
	state             uint32
	errors, successes int
	lastError         time.Time
}

func New(errorThreshold, successThreshold int, timeout time.Duration) *Breaker {
	return &Breaker{
		errorThreshold:   errorThreshold,
		successThreshold: successThreshold,
		timeout:          timeout,
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

	if result == nil && panicValue == nil {
		return nil
	}

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

	for{
		signal := <-monitorCh
		log.Println("monitor.goroutine: recieved signal", signal)
		if signal != NotProcessed {
			cnt = 0
			chosenCnt = int(math.Ceil(factor*float64(size)))
		}

		switch signal {
		case Error:
			if b.errors > 0 {
				expiry := b.lastError.Add(b.timeout)
				if time.Now().After(expiry) {
					b.errors = 0
				}
			}
			switch b.state {
			case Closed:
				b.errors++
				if b.errors == b.errorThreshold {
					b.openBreaker(breakerChs, size, chosenCnt)
				} else {
					b.lastError = time.Now()
				}
			case HalfOpen:
				b.openBreaker(breakerChs, size, chosenCnt)
			}
		case Success:
			if b.state == HalfOpen {
				b.successes++
				if b.successes == b.successThreshold {
					b.closeBreaker(breakerChs, size)
				}
			}
		case NotProcessed:
			cnt++
			if cnt != chosenCnt {
				continue
			}
			if cnt == chosenCnt{
				chosenCnt = int(math.Min(float64(size), float64(2*chosenCnt)))
				cnt = 0
				log.Printf("monitor.goroutine: sending signal %v to %v goroutines\n", Play, chosenCnt)
				sendSignal(Play, chosenCnt, breakerChs)
			}
		}
	}
}

func (b *Breaker) openBreaker(breakerChs []chan uint32, size int, chosenCnt int) {
	b.changeState(Open)
	sendSignal(Pause, size, breakerChs)
	go b.timer(breakerChs, chosenCnt)
}

func (b *Breaker) closeBreaker(breakerChs []chan uint32, size int) {
	b.changeState(Closed)
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
	b.errors = 0
	b.successes = 0

	atomic.StoreUint32(&b.state, newState)

	log.Println("monitor.goroutine: status of breaker is ", newState)
}

func sendSignal(signal uint32, chosenCnt int, breakerChs []chan uint32) {
	for i := 0; i < chosenCnt; i++ {
		breakerChs[i] <- signal
	}
}