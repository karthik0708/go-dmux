package http

import (
	"bytes"
	"errors"
	"fmt"
	breaker "github.com/go-dmux/breaker"
	"io"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"strconv"
	"time"

	core "github.com/go-dmux/core"
)

//HTTPSink is Sink implementation which writes to HttpEndpoint
type HTTPSink struct {
	client *http.Client
	hook   HTTPSinkHook
	conf   HTTPSinkConf
	cirBreaker *breaker.Breaker
}

//HTTPSinkConf  holds config to HTTPSink
type HTTPSinkConf struct {
	Endpoint                    string              `json:"endpoint"` //http://destinationHost:port/prefixPath
	Timeout                     core.Duration       `json:"timeout"`
	RetryInterval               core.Duration       `json:"retry_interval"`
	Headers                     []map[string]string `json:"headers"`
	Method                      string              `json:"method"`                    //GET,POST,PUT,DELETE
	NonRetriableHttpStatusCodes []int               `json:nonRetriableHttpStatusCodes` //this is for handling customized errorCode thrown by sink
	ErrorThreshold  			int 			    `json:"error_threshold"`
	SuccessThreshold 			int 			    `json:"success_threshold"`
	BreakerTimeout  			core.Duration       `json:"breaker_timeout"`
}

//HTTPSinkHook is added for Clien to attach pre and post porcessing logic
type HTTPSinkHook interface {
	PreHTTPCall(msg interface{})
	PostHTTPCall(msg interface{}, sucess bool)
}

func getHTTPClientTransport(size int, conf HTTPSinkConf) http.RoundTripper {
	//defaults copied from DefaultTransport logic in docs https://golang.org/pkg/net/http/
	return &http.Transport{
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
			DualStack: true,
		}).DialContext,
		MaxIdleConns:          getMaxConn(size),
		MaxIdleConnsPerHost:   getMaxConn(size),
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

}

func getMaxConn(size int) int {
	if size < 2 {
		return 2
	}
	return size
}

func getClientTimeout(conf HTTPSinkConf) time.Duration {
	defaultTimeout := 10 * time.Second
	noTimeout := 10 * time.Nanosecond
	if conf.Timeout.Duration == noTimeout {
		return defaultTimeout
	}
	return conf.Timeout.Duration
}

//GetHTTPSink method is public method used to create Instance of HTTPSink
func GetHTTPSink(size int, conf HTTPSinkConf) *HTTPSink {

	client := &http.Client{
		Transport: getHTTPClientTransport(size, conf),
		Timeout:   getClientTimeout(conf),
	}

	sink := &HTTPSink{
		client: client,
		conf:   conf,
	}

	return sink
}

func (h *HTTPSink) RegisterHook(hook HTTPSinkHook) {
	h.hook = hook
}

//HTTPMsg is an interface which incoming data should implment for HttpSink to
//work
type HTTPMsg interface {
	GetPayload() []byte

	GetDebugPath() string
	GetURL(endpoint string) string

	//static methods
	GetHeaders(conf HTTPSinkConf) map[string]string
	BatchURL(msgs []interface{}, endpoint string, version int) string
	BatchPayload(msgs []interface{}, version int) []byte
}

//Clone is implementation of Sink interface method. As HTTPSink is Stateless
//this method returns selfRefrence
func (h *HTTPSink) Clone() core.Sink {
	return h
}

//BatchConsume is implementation of Sink interface Consume.
func (h *HTTPSink) BatchConsume(msgs []interface{}, version int, breakerCh <- chan uint32,  monitorCh chan<- uint32) {
	// fmt.Println(msgs)
	batchHelper := msgs[0].(HTTPMsg) // empty refrence to help call static methods
	// data := msg.(HTTPMsg)

	url := batchHelper.BatchURL(msgs, h.conf.Endpoint, version)
	payload := batchHelper.BatchPayload(msgs, version)
	headers := batchHelper.GetHeaders(h.conf)

	//TODO introduce batchHookMethods
	for _, msg := range msgs {
		//retry Pre till you succede infinitely
		h.retryPre(msg, url)
	}

	//retry Execute till you succede based on retry config
	status := h.retryExecute(h.conf.Method, url, headers, payload, responseCodeEvaluation, breakerCh, monitorCh)

	for _, msg := range msgs {
		//retry Post till you succede infinitely
		h.retryPost(msg, status, url)
	}

}

//Consume is implementation for Single message Consumption.
//This infinitely retries pre and post hooks, but finetly retries HTTPCall
//for status. status == true is determined by responseCode 2xx
func (h *HTTPSink) Consume(msg interface{}, breakerCh <- chan uint32,  monitorCh chan<- uint32) {

	data := msg.(HTTPMsg)
	url := data.GetURL(h.conf.Endpoint)
	// method := data.GetMethod(h.conf)
	payload := data.GetPayload()
	headers := data.GetHeaders(h.conf)
	//retry Pre till you succede infinitely
	h.retryPre(msg, url)

	//retry Execute till you succede based on retry config
	status := h.retryExecute(h.conf.Method, url, headers, payload, responseCodeEvaluation, breakerCh, monitorCh)

	//retry Post till you succede infinitely
	h.retryPost(msg, status, url)

}

//InitBreaker is for initializing the breaker using HTTPSink config
func (h *HTTPSink) InitBreaker(){
	h.cirBreaker = breaker.New(h.conf.ErrorThreshold, h.conf.SuccessThreshold, h.conf.BreakerTimeout.Duration)
}

//PlaceBreaker puts a breaker on the critical function which returns an error  T
//The breaker is opened once error threshold is reached
func (h *HTTPSink) PlaceBreaker(critical func () error, monitorCh chan<- uint32) error{
	return h.cirBreaker.Run(critical, monitorCh)
}

func (h *HTTPSink) retryPre(msg interface{}, url string) {
	for {
		status := h.pre(h.hook, msg, url)
		if status {
			break
		}
		log.Println("retry in http_sink pre ", url)
		time.Sleep(h.conf.RetryInterval.Duration)
	}
}

func (h *HTTPSink) retryPost(msg interface{}, state bool,
	url string) {
	for {
		status := h.post(h.hook, msg, state, url)
		if status {
			break
		}
		log.Println("retry in http_sink post ", url)
		time.Sleep(h.conf.RetryInterval.Duration)
	}

}

//retryExecute implements a circuit breaker to provide a guardrail
func (h *HTTPSink) retryExecute(method, url string, headers map[string]string, data []byte,
	respEval func(respCode int, nonRetriableHttpStatusCodes []int) (error, bool),
	breakerCh <- chan uint32,  monitorCh chan<- uint32) bool {

	outcome := false
	currentStatus := breaker.Closed
	for {
		select {
		case signal := <-breakerCh:
			log.Println("sink: recieved from monitor",signal, url)
			currentStatus = signal
		default:
			if currentStatus == breaker.HalfOpen || currentStatus == breaker.Closed {
				err := h.PlaceBreaker(func() error {
					status, respCode := h.execute(method, url, headers, bytes.NewReader(data))
					if status {
						nonRetriableHttpStatusCodes := h.conf.NonRetriableHttpStatusCodes
						err, tmp := respEval(respCode, nonRetriableHttpStatusCodes)
						outcome = tmp
						return err
					} else {
						return errors.New("execution failed")
					}
				}, monitorCh)
				switch err {
				case nil:
					return outcome
				case breaker.ErrBreakerOpen:
					if currentStatus!=breaker.Open {
						currentStatus = breaker.Open
						log.Printf("Circuit is now open \t %s \n", url)
					}
				default:
					log.Printf("retry in execute %s \t %s\n", method, url)
					time.Sleep(h.conf.RetryInterval.Duration)
				}
			}
		}
	}
}

func (h *HTTPSink) pre(hook HTTPSinkHook, msg interface{}, url string) bool {
	// PreProcess
	defer func() {
		if r := recover(); r != nil {
			log.Printf("failed in httpsink pre hook %s %v", url, r)
		}
	}()

	if hook != nil {
		hook.PreHTTPCall(msg)
	}
	return true
}

func (h *HTTPSink) post(hook HTTPSinkHook, msg interface{}, status bool, url string) bool {
	// PostPorcessing
	defer func() {
		if r := recover(); r != nil {
			log.Printf("failed in httpsink post hook %s %v", url, r)
		}
	}()

	if hook != nil {
		hook.PostHTTPCall(msg, status)
	}
	return true
}

func (h *HTTPSink) execute(method, url string, headers map[string]string,
	payload io.Reader) (bool, int) {
	//Never fail always recover
	defer func() {
		if r := recover(); r != nil {
			fmt.Printf("recovered in execute %s %v", url, r)
		}
	}()

	//build request
	request, err := http.NewRequest(method, url, payload)
	if err != nil {
		log.Printf("failed in request build %s %s \n", url, err.Error())
		return false, 0
	}

	//set headers
	for key, val := range headers {
		request.Header.Set(key, val)
	}

	// if method != "GET" && !h.conf.CustomURL {
	// 	request.Header.Set("Content-Type", "application/octet-stream")
	// }

	//make request
	response, err := h.client.Do(request)
	if err != nil {
		log.Printf("failed in http call invoke %s %s \n", url, err.Error())
		return false, 0
	}
	//TODO check if this can be avoided
	io.Copy(ioutil.Discard, response.Body)
	defer response.Body.Close()

	return true, response.StatusCode
}

func responseCodeEvaluation(respCode int, nonRetriableHttpStatusCodes []int) (error, bool) {
	if (respCode < 300) || core.Contains(nonRetriableHttpStatusCodes, respCode) { //2xx or ay http status defined in nonRetriableHttpStatusCodes status implies sucess
		return nil, true
	}
	return errors.New(strconv.Itoa(respCode)), false //all other status code mean error
}
