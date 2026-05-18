package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sync"
	"time"

	probing "github.com/prometheus-community/pro-bing"
)

func main() {
	ipsFile := flag.String("f", "", "File containing list of IPs or CIDRs to scan (use '-' for stdin)")
	workers := flag.Int("w", 1000, "Number of concurrent workers")
	timeout := flag.Duration("t", time.Second, "Ping timeout")
	privileged := flag.Bool("p", false, "Use privileged mode (raw sockets)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] [ips_file]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	var inputSource *os.File
	var err error

	argFile := ""
	if flag.NArg() > 0 {
		argFile = flag.Arg(0)
	} else {
		argFile = *ipsFile
	}

	if argFile == "" || argFile == "-" {
		// Check if stdin has data (is piped or redirected)
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) == 0 {
			inputSource = os.Stdin
		} else if argFile == "-" {
			inputSource = os.Stdin
		} else {
			// Default fallback if nothing is piped and no file is provided
			argFile = "ips.list"
		}
	}

	if inputSource == nil {
		inputSource, err = os.Open(argFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening file %s: %v\n", argFile, err)
			os.Exit(1)
		}
		defer inputSource.Close()
	}

	// Auto-detect privileged mode if not specified
	isPrivileged := *privileged
	if !isPrivileged {
		if !checkUnprivileged() {
			isPrivileged = true
		}
	}

	jobs := make(chan string, *workers*2)
	results := make(chan pingResult, *workers*2)
	var wg sync.WaitGroup

	// Start workers
	for w := 1; w <= *workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ip := range jobs {
				results <- pingIP(ip, *timeout, isPrivileged)
			}
		}()
	}

	// Receiver/Printer goroutine
	done := make(chan bool)
	go func() {
		for res := range results {
			if res.up {
				fmt.Println(res.ip)
			}
		}
		done <- true
	}()

	// Producer: Stream IPs from input source
	streamIPs(inputSource, jobs)
	close(jobs)

	wg.Wait()
	close(results)
	<-done
}

func checkUnprivileged() bool {
	pinger, err := probing.NewPinger("127.0.0.1")
	if err != nil {
		return false
	}
	pinger.Count = 1
	pinger.Timeout = time.Millisecond * 100
	pinger.SetPrivileged(false)
	err = pinger.Run()
	return err == nil
}

func streamIPs(input io.Reader, jobs chan<- string) {
	scanner := bufio.NewScanner(input)
	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		if ip, ipnet, err := net.ParseCIDR(line); err == nil {
			for ip := ip.Mask(ipnet.Mask); ipnet.Contains(ip); inc(ip) {
				jobs <- ip.String()
			}
		} else {
			jobs <- line
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
	}
}

type pingResult struct {
	ip string
	up bool
}

func pingIP(ip string, timeout time.Duration, privileged bool) pingResult {
	pinger, err := probing.NewPinger(ip)
	if err != nil {
		return pingResult{ip, false}
	}

	pinger.Count = 1
	pinger.Timeout = timeout
	pinger.SetPrivileged(privileged)

	err = pinger.Run()
	if err != nil {
		return pingResult{ip, false}
	}

	stats := pinger.Statistics()
	return pingResult{ip, stats.PacketsRecv > 0}
}

func inc(ip net.IP) {
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] > 0 {
			break
		}
	}
}
