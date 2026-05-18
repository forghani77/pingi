package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"

	probing "github.com/prometheus-community/pro-bing"
)

func main() {
	ipsFile := flag.String("f", "", "File containing list of IPs or CIDRs to scan (use '-' for stdin)")
	workers := flag.Int("w", 1000, "Number of concurrent workers")
	timeoutRaw := flag.String("t", "1s", "Ping timeout (e.g., 1s, 500ms, or 1000 for 1000ms)")
	privileged := flag.Bool("p", false, "Use privileged mode (raw sockets)")
	showLatency := flag.Bool("l", false, "Display latency")
	minLatencyRaw := flag.String("min", "0", "Filter results with latency greater than or equal to this (e.g., 20ms or 20 for 20ms)")
	sortByLatency := flag.Bool("sort", false, "Sort results by latency (disables streaming output)")

	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s [options] [ips_file]\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "Options:\n")
		flag.PrintDefaults()
	}

	flag.Parse()

	tVal, err := parseDuration(*timeoutRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid timeout: %v\n", err)
		os.Exit(1)
	}
	timeout := &tVal

	mVal, err := parseDuration(*minLatencyRaw)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Invalid min latency: %v\n", err)
		os.Exit(1)
	}
	minLatency := &mVal

	var inputSource *os.File

	argFile := ""
	if flag.NArg() > 0 {
		argFile = flag.Arg(0)
	} else {
		argFile = *ipsFile
	}

	if argFile == "-" {
		inputSource = os.Stdin
	} else if argFile != "" {
		inputSource, err = os.Open(argFile)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error opening file %s: %v\n", argFile, err)
			os.Exit(1)
		}
		defer inputSource.Close()
	} else {
		// No file or flag provided, check stdin
		stat, _ := os.Stdin.Stat()
		if (stat.Mode() & os.ModeCharDevice) == 0 {
			inputSource = os.Stdin
		} else {
			// No piped data and no file provided
			fmt.Fprintf(os.Stderr, "Error: No input provided. Use -f, a filename argument, or pipe data to stdin.\n")
			os.Exit(1)
		}
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

	var collectedResults []pingResult
	var mu sync.Mutex

	// Receiver/Printer goroutine
	done := make(chan bool)
	go func() {
		for res := range results {
			if !res.up {
				continue
			}
			if *minLatency > 0 && res.latency < *minLatency {
				continue
			}

			if *sortByLatency {
				mu.Lock()
				collectedResults = append(collectedResults, res)
				mu.Unlock()
			} else {
				printResult(res, *showLatency)
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

	if *sortByLatency {
		sort.Slice(collectedResults, func(i, j int) bool {
			return collectedResults[i].latency < collectedResults[j].latency
		})
		for _, res := range collectedResults {
			printResult(res, *showLatency)
		}
	}
}

func printResult(res pingResult, showLatency bool) {
	if showLatency {
		fmt.Printf("%s\t%v\n", res.ip, res.latency)
	} else {
		fmt.Println(res.ip)
	}
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
	ip      string
	up      bool
	latency time.Duration
}

func pingIP(ip string, timeout time.Duration, privileged bool) pingResult {
	pinger, err := probing.NewPinger(ip)
	if err != nil {
		return pingResult{ip, false, 0}
	}

	pinger.Count = 1
	pinger.Timeout = timeout
	pinger.SetPrivileged(privileged)

	err = pinger.Run()
	if err != nil {
		return pingResult{ip, false, 0}
	}

	stats := pinger.Statistics()
	return pingResult{ip, stats.PacketsRecv > 0, stats.AvgRtt}
}

func inc(ip net.IP) {
	for j := len(ip) - 1; j >= 0; j-- {
		ip[j]++
		if ip[j] > 0 {
			break
		}
	}
}

func parseDuration(s string) (time.Duration, error) {
	d, err := time.ParseDuration(s)
	if err == nil {
		return d, nil
	}
	// Try parsing as raw number (milliseconds)
	ms, err := strconv.ParseInt(s, 10, 64)
	if err == nil {
		return time.Duration(ms) * time.Millisecond, nil
	}
	return 0, err
}
