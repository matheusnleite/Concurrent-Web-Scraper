package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
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

func fetchURL(wg *sync.WaitGroup, jobs <-chan Job, channel chan Result, ctx context.Context) {
	defer wg.Done() //vai diminuir o contador do waitGroup quando a funçao terminar

	//url para testar log de timeout https://httpbin.org/delay/5
	client := http.Client{
		Timeout: 5 * time.Second,
	}

	for job := range jobs {
		if err := ctx.Err(); err != nil {
			reason := "Global timeout exceeded"
			if errors.Is(err, context.Canceled) {
				reason = "Stopped by User (Ctrl+C)"
			}
			channel <- Result{URL: job.URL, Status: reason}
			break
		}
		start := time.Now()                                                       //tempo agora
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, job.URL, nil) //requisicao get
		if err != nil {
			channel <- Result{URL: job.URL, Status: "Error creating request"}
			continue
		}

		resp, err := client.Do(req) //client executa a requisiçao com o timeout global implementado
		if err != nil {
			if errors.Is(err, context.Canceled) {
				channel <- Result{URL: job.URL, Status: "Stopped by User (Ctrl + C)"}
				continue
			}
			reason := "Error: " + err.Error()
			if netErr, ok := errors.AsType[net.Error](err); ok && netErr.Timeout() {
				reason = "Error: the request timed out"
			}
			if dnsErr, ok := errors.AsType[*net.DNSError](err); ok {
				reason = "Error: DNS failure - " + dnsErr.Error()
			}
			channel <- Result{URL: job.URL, Status: reason, duration: time.Since(start)} //retorna erro no canal
			continue
		}

		duration := time.Since(start)
		status := resp.Status
		if resp.StatusCode == http.StatusNotFound {
			status = "Bad status code: 404 - the requested URL was not found"
		} else if resp.StatusCode == http.StatusInternalServerError {
			status = "Bad status code: 500 - Internal Server Error"
		}
		channel <- Result{job.URL, status, duration} //retorna os resultados no canal
		resp.Body.Close()                            //fecha o response body
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

	//criando contexto caso o usuário pressione Ctrl + C
	ctxStop, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	//criando contexto para dicionar timeout global
	ctx, cancel := context.WithTimeout(ctxStop, 30*time.Second)
	defer cancel()

	fmt.Println("Application started. Press Ctrl+C to exit.")

	for w := 0; w < numWorkers; w++ {
		wg.Add(1)                            //incrementando o contador antes de chamar a goroutine
		go fetchURL(&wg, jobs, results, ctx) //goroutine que manda o endereço de memoria do waitGroup e os canais para processar e receber a resposta
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
		fmt.Println(result.Status)
	}

	switch {
	case ctxStop.Err() != nil:
		fmt.Println("\n[!] Ctrl+C intercepted! Application stopped gracefully.")
	case ctx.Err() == context.DeadlineExceeded:
		fmt.Println("\n[!] Global timeout of 30s exceeded.")
	default:
		fmt.Println("\nAll URLs processed.")
	}

}
