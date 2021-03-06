package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
)

var (
	possibleFirstName = [...]string{"Joe", "Marcus", "Peter", "Mike", "Steve", "Donald", "David"}
	possibleLastName  = [...]string{"Biden", "Trump", "Peterson", "Stewart", "Merkel"}
	r                 map[string]interface{}
	wg                sync.WaitGroup
	cfg               = elasticsearch.Config{
		Addresses: []string{
			"http://localhost:9200",
			//"http://localhost:9201",
		}}
	es, _               = elasticsearch.NewClient(cfg)
	ch                  = make(chan struct{})
	chShutdown          = make(chan struct{})
	globalGoroutinePool = 100
	globalDocsToIndex   = 10000
	mtx                 = sync.Mutex{}
)

func stressTest() {
	defer func() { ch <- struct{}{} }()
	defer wg.Done()
	defer reduceDocsToIndex()

	rand.Seed(time.Now().Unix())
	firstName := possibleFirstName[rand.Intn(len(possibleFirstName))]
	lastName := possibleLastName[rand.Intn(len(possibleLastName))]
	fullName := fmt.Sprintf("%s %s", firstName, lastName)
	fmt.Println(fullName)

	// Build the request body.
	var b strings.Builder
	b.WriteString(`{"first" : "`)
	b.WriteString(firstName + `",`)
	b.WriteString(`"last" : "`)
	b.WriteString(lastName + `",`)
	b.WriteString(`"goals" : "`)
	b.WriteString(`[0,1,2]",`)
	b.WriteString(`"assists" : "`)
	b.WriteString(`[0,1,2],"`)
	b.WriteString(`"gp" : "`)
	b.WriteString(`[0,1,2]",`)
	b.WriteString(`"born" : "`)
	b.WriteString(`2000/12/15`)
	b.WriteString(`"}`)

	// Set up the request object.
	req := esapi.IndexRequest{
		Index:   "hockey",
		Body:    strings.NewReader(b.String()),
		Refresh: "true",
	}

	// Perform the request with the client.
	res, err := req.Do(context.Background(), es)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
	}
	defer res.Body.Close()
	defer io.Copy(ioutil.Discard, res.Body)

	if res.IsError() {
		log.Printf("[%s] Error indexing document fullName=%s", res.Status(), fullName)
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and indexed document version.
			log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
		}
	}

}

func reduceDocsToIndex() {
	mtx.Lock()
	globalDocsToIndex--
	mtx.Unlock()
}

func coordinator() {
	fmt.Println("Coordinator Starting.")
Loop:
	for {
		select {
		case <-ch:
			fmt.Println("Coordinator: Calling one more worker")
			if globalDocsToIndex > 0 {
				wg.Add(1)
				go stressTest()
			}
		case <-chShutdown:
			fmt.Println("Coordinator dying. Goodbye")
			wg.Done()
			break Loop
		}
	}
}

func main() {
	fmt.Println("stressTest on ES database is starting! hit ctrl+c to stop it!")
	log.Println(elasticsearch.Version)
	//log.Println(es.Info())
	wg.Add(1)
	go coordinator()
	start := time.Now()
	for i := 0; i <= globalGoroutinePool; i++ {
		wg.Add(1)
		go stressTest()
	}
	wg.Wait()

	elapsed := (time.Since(start))
	fmt.Println("Stress test finished")

	fmt.Println("Current test lasted for ", elapsed)
}
