package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

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

func fetchURL(url string) (string, time.Duration) {
	start := time.Now() //tempo agora

	//url para testar log de timeout https://httpbin.org/delay/5
	client := http.Client{
		Timeout: 2 * time.Second,
	}

	resp, err := client.Get(url) //requisicao get
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
		return "", time.Since(start)
	}
	duration := time.Since(start)

	if resp.StatusCode >= 400 {
		switch resp.StatusCode {
		case http.StatusNotFound:
			fmt.Println("Bad status code: 404 - the requested URL was not found")
		case http.StatusInternalServerError:
			fmt.Println("Bad status code: 500 - Internal Server Error")
		default:
			fmt.Printf("Bad status code: %d\n", resp.StatusCode)
		}
	}
	defer resp.Body.Close() //fecha o response body

	return resp.Status, duration
}

func main() {
	filePath := "urls.txt"
	urls := readFile(filePath)

	for _, url := range urls {
		url = strings.TrimSpace(url) //limpa a url para fazer a requisicao
		fmt.Printf("Fetching %s\n", url)
		statusCode, duration := fetchURL(url)
		fmt.Println(statusCode, "\n", duration)
	}

}
