package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

var (
	numWorkers int
)

type Result struct { //struct para conter os dois valores da resposta da função de fetch
	URL      string
	Status   string
	duration time.Duration
}

type Job struct {
	URL string
}

func readFile(filePath string) []string {

	urls := make([]string, 0)      //slice que vai salvar as urls do arquivo
	file, err := os.Open(filePath) //abrindo arquivo txt
	if err != nil {
		log.Fatal("Error opening file", err)
	}
	defer file.Close() //executa após a funcão main terminar

	scanner := bufio.NewReader(file) //buffer do arquivo

	for {
		textLine, err := scanner.ReadString('\n')
		if err == io.EOF {
			if len(textLine) != 0 {
				urls = append(urls, textLine) //adiciona a ultima linha
			}
			break
		}
		if err != nil {
			log.Fatal("Error reading file:", err)
		}
		urls = append(urls, textLine)
	}
	return urls
}

func fetchURL(wg *sync.WaitGroup, jobs <-chan Job, channel chan Result) {
	defer wg.Done() //vai diminuir o contador do waitGroup quando a funçao terminar

	//url para testar log de timeout https://httpbin.org/delay/5
	client := http.Client{
		Timeout: 2 * time.Second,
	}
	for job := range jobs {
		start := time.Now()              //tempo agora
		resp, err := client.Get(job.URL) //requisicao get
		if err != nil {
			if netErr, ok := errors.AsType[net.Error](err); ok {
				if netErr.Timeout() {
					fmt.Println("Network error: the request timed out")
				} else {
					fmt.Println("Network error: General network/connectivity failure")
				}
			}
			if dnsErr, ok := errors.AsType[*net.DNSError](err); ok {
				fmt.Println("Network error: DNS failure", dnsErr)
			}
			channel <- Result{Status: "Error", duration: time.Since(start)} //retorna erro no canal
			continue
		}

		duration := time.Since(start)

		if resp.StatusCode >= 400 {
			switch resp.StatusCode {
			case http.StatusNotFound:
				fmt.Println("Bad status code: 404 - the requested URL was not found")
			case http.StatusInternalServerError:
				fmt.Println("Bad status code: 500 - Internal Server Error")
			}
		}
		channel <- Result{job.URL, resp.Status, duration} //retorna os resultados no canal
		resp.Body.Close()                                 //fecha o response body
	}
}

func main() {

	flag.IntVar(&numWorkers, "n", 1, "Number of workers")
	flag.Parse()
	filePath := "urls.txt"
	urls := readFile(filePath)

	jobs := make(chan Job, len(urls)) // canal com a fila das urls
	results := make(chan Result)      //criando canal para as goroutines executarem
	var wg sync.WaitGroup             //criando o wait group

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)                       //incrementando o contador antes de chamar a goroutine
		go fetchURL(&wg, jobs, results) //goroutine que manda o endereço de memoria do waitGroup e os canais para processar e receber a resposta
	}

	for _, url := range urls {

		url = strings.TrimSpace(url) //limpa a url para fazer a requisicao
		fmt.Printf("%s added to the queue\n", url)
		jobs <- Job{url}

	}
	close(jobs)

	go func() {
		wg.Wait() //espera todas as funçoes terminarem
		close(results)
	}()

	for result := range results {
		fmt.Printf("\n%s finished in %s\n", result.URL, result.duration)
		fmt.Println(result.Status, "\n")
	}
}
