package main

import (
	"fmt"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

const (
	threads = 150
	count   = 1_000_000
)

func main() {
	var urls []string
	for i := 0; i < count; i++ {
		u := rand.Int31() % 1024
		urls = append(urls, fmt.Sprintf("http://localhost:8080/rate/foo/%d", u))
	}

	wg := sync.WaitGroup{}

	start := time.Now()
	for i := 0; i < threads; i++ {
		wg.Add(1)
		local := urls[i*(count/threads) : (i+1)*(count/threads)]
		client := http.Client{}

		go func() {
			for j := range local {
				client.Get(local[j])
			}
			wg.Done()
		}()
	}

	wg.Wait()
	done := time.Now()
	total := done.Sub(start)

	fmt.Printf("took %d milliseconds to do %d requests in %d goroutines:\n \t - %f per second\n", total.Milliseconds(), threads, count, count/total.Seconds())
}
