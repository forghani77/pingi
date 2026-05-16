package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"sync"
	"time"

	probing "github.com/prometheus-community/pro-bing"
)

func main() {
	ipsFile := flag.String("f", "ips.list", "File containing list of IPs or CIDRs to scan")
	workers := flag.Int("w", 1000, "Number of concurrent workers")
	timeout := flag.Duration("t", time.Second, "Ping timeout")
	privileged := flag.Bool("p", false, "Use privileged mode (raw sockets)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] [ips_file]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	if flag.NArg() > 0 {
		*ipsFile = flag.Arg(0)
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

	// Producer: Stream IPs from file
	err := streamIPs(*ipsFile, jobs)
	if err != nil {
		fmt.Printf("Error streaming IPs: %v\n", err)
	}
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

func streamIPs(ipsFile string, jobs chan<- string) error {
	file, err := os.Open(ipsFile)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
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
	return scanner.Err()
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
