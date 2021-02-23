package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"time"
)

const ServerPort = 62985
const FetchURLTimeOut = 1 * time.Second
const MaxSimultaneousRequests = 100
const MaxURLs = 20
const MaxSimultaneousConnectionsPerRequest = 4

// 2048 chars max URL length; 4 service chars for each URL; + 3 service chars for request
// NOTE: increase MaxBodySize limit if you add something to request body
const MaxBodySize = MaxURLs*(2048+4) + 3

type URL string

type Job struct {
	URL    URL `json:"url"`
	Status int `json:"status"`
}

func main() {
	reqSemaphore := make(chan struct{}, MaxSimultaneousRequests)

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		log.Print("got new request")

		reqSemaphore <- struct{}{}
		defer func() { <-reqSemaphore }()

		w.Header().Set("Content-Type", "application/json; charset=UTF-8")

		var urls []URL

		body, err := ioutil.ReadAll(io.LimitReader(r.Body, MaxBodySize))
		if err != nil {
			w.WriteHeader(400)
			log.Printf("client error: %s", err)
			return
		}
		closeReqBody(r)

		err = json.Unmarshal(body, &urls)
		if err == nil && len(urls) > MaxURLs {
			err = fmt.Errorf("there is to much urls, please send no more than 20")
		}
		if err != nil {
			setClientError(w, err)
			return
		}

		urlSemaphore := make(chan struct{}, MaxSimultaneousConnectionsPerRequest)
		wg := new(sync.WaitGroup)
		errChan := make(chan error)
		finChan := make(chan bool)

		wg.Add(len(urls))

		var jobs []*Job

		for _, url := range urls {
			job := Job{URL: url}
			jobs = append(jobs, &job)
			go fetchURL(&job, r.Context(), urlSemaphore, wg, errChan)
		}

		go func() {
			wg.Wait()
			finChan <- true
		}()

		select {
		case err := <-errChan:
			r.Context().Done()
			setClientError(w, err)
			return
		case <-finChan:
			log.Println("done")

			w.WriteHeader(http.StatusOK)
			err = json.NewEncoder(w).Encode(jobs)
			checkError(err)
		}
	})

	server := &http.Server{Addr: fmt.Sprintf(":%d", ServerPort)}
	go func() {
		err := server.ListenAndServe()
		if err != nil && err.Error() == "http: Server closed" {
			log.Println(err)
			return
		}
		checkError(err)
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := server.Shutdown(ctx)
	checkError(err)
}

func fetchURL(job *Job, ctx context.Context, sem chan struct{}, wg *sync.WaitGroup, errChan chan error) {
	sem <- struct{}{}
	defer func() { <-sem }()

	client := http.Client{Timeout: FetchURLTimeOut}

	req, err := http.NewRequest(http.MethodGet, string(job.URL), nil)
	if err != nil {
		errChan <- err
		return
	}
	res, err := client.Do(req.WithContext(ctx))
	if err != nil {
		errChan <- err
		return
	}

	log.Printf("got res status code: %d", res.StatusCode)

	job.Status = res.StatusCode
	wg.Done()
	return
}

func setClientError(w http.ResponseWriter, err error) {
	log.Printf("client error: %s", err)

	w.WriteHeader(404)
	_, err = w.Write([]byte(err.Error()))
	checkError(err)
}

func checkError(err error) {
	if err != nil {
		panic(err)
	}
}

func closeReqBody(r *http.Request) {
	err := r.Body.Close()
	checkError(err)
}
