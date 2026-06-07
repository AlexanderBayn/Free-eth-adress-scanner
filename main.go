package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

const (
	configFileName           = "config.json"
	addressesFileName        = "addresses.txt"
	positiveBalancesFileName = "positive_balances.txt"
	logFileName              = "errors.log"
)

// Config now includes the configurable requests per second rate limit
type Config struct {
	APIKey            string `json:"api_key"`
	RequestsPerSecond int    `json:"requests_per_second"`
}

type AccountBalance struct {
	Account string `json:"account"`
	Balance string `json:"balance"`
}

type EtherscanMultiResponse struct {
	Status  string          `json:"status"`
	Message string          `json:"message"`
	Result  json.RawMessage `json:"result"`
}

type FileInitResult struct {
	FileType string
	Created  bool
	Err      error
}

func main() {
	fmt.Println("Initializing workspace files simultaneously...")

	defer func() {
		fmt.Println("\nPress Enter to close the program...")
		bufio.NewReader(os.Stdin).ReadString('\n')
	}()

	logFile, err := os.OpenFile(logFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Printf("Error opening log file: %v\n", err)
		return
	}
	defer logFile.Close()

	var wg sync.WaitGroup
	resultChan := make(chan FileInitResult, 2)

	wg.Add(2)
	go func() {
		defer wg.Done()
		created, err := ensureConfig()
		resultChan <- FileInitResult{FileType: "config", Created: created, Err: err}
	}()

	go func() {
		defer wg.Done()
		created, err := ensureAddresses()
		resultChan <- FileInitResult{FileType: "addresses", Created: created, Err: err}
	}()

	wg.Wait()
	close(resultChan)

	stopExecution := false
	for res := range resultChan {
		if res.Err != nil {
			msg := fmt.Sprintf("[-] Error initializing %s: %v", res.FileType, res.Err)
			fmt.Println(msg)
			appendLog(logFile, msg)
			stopExecution = true
			continue
		}
		if res.Created {
			stopExecution = true
		}
	}

	if stopExecution {
		fmt.Println("\nAction Required: Setup files have been configured/verified. Please review 'config.json' and 'addresses.txt' before restarting.")
		return
	}

	cfg, err := readConfig(configFileName)
	if err != nil {
		msg := fmt.Sprintf("Error reading config: %v", err)
		fmt.Printf("%s\n", msg)
		appendLog(logFile, msg)
		return
	}

	if cfg.APIKey == "YOUR_ETHERSCAN_API_KEY_HERE" || cfg.APIKey == "" {
		fmt.Printf("Action Required: Update your Etherscan API key in '%s'.\n", configFileName)
		return
	}

	// Fallback to safe defaults if configured incorrectly
	if cfg.RequestsPerSecond <= 0 {
		cfg.RequestsPerSecond = 5
	}

	if cfg.RequestsPerSecond > 5 {
		fmt.Printf("Warning: RequestsPerSecond is set to %d, which exceeds the recommended limit of 5. Press Enter to continue or Ctrl+C to abort.", cfg.RequestsPerSecond)
		bufio.NewReader(os.Stdin).ReadString('\n')
	}

	addresses, err := readAddresses(addressesFileName)
	if err != nil {
		msg := fmt.Sprintf("Error reading addresses: %v", err)
		fmt.Printf("%s\n", msg)
		appendLog(logFile, msg)
		return
	}

	if len(addresses) == 0 {
		fmt.Printf("The file '%s' is empty.\n", addressesFileName)
		return
	}

	outFile, err := os.OpenFile(positiveBalancesFileName, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		msg := fmt.Sprintf("Error opening '%s': %v", positiveBalancesFileName, err)
		fmt.Printf("%s\n", msg)
		appendLog(logFile, msg)
		return
	}
	defer outFile.Close()

	// Calculate dynamic delay per batch request based on config file rate limits
	// e.g., 5 requests per second = 1000ms / 5 = 200ms sleep between calls
	rateLimitDelay := time.Duration(1000/cfg.RequestsPerSecond) * time.Millisecond

	fmt.Printf("\nStarting multi-balance checks for %d address(es)...\n", len(addresses))
	fmt.Printf("Rate limit rule: Max %d requests per second (Delay: %v between calls)\n", cfg.RequestsPerSecond, rateLimitDelay)
	fmt.Println(strings.Repeat("-", 60))

	const chunkSize = 20
	for i := 0; i < len(addresses); i += chunkSize {
		end := i + chunkSize
		if end > len(addresses) {
			end = len(addresses)
		}
		chunk := addresses[i:end]

		// Throttling step using config value
		if i > 0 {
			time.Sleep(rateLimitDelay)
		}

		fmt.Printf("Fetching batch %d (Addresses %d to %d)...\n", (i/chunkSize)+1, i+1, end)
		balances, err := fetchMultiBalances(chunk, cfg.APIKey)
		if err != nil {
			msg := fmt.Sprintf("[-] Error fetching batch: %v", err)
			fmt.Printf("%s\n\n", msg)
			appendLog(logFile, msg)
			if isFatalAPIKeyError(err) {
				fmt.Println("Fatal: API key failure detected. Aborting.")
				appendLog(logFile, "Fatal: API key failure detected. Aborting.")
				return
			}
			continue
		}

		for _, accBal := range balances {
			balanceWei := new(big.Int)
			if _, ok := balanceWei.SetString(accBal.Balance, 10); !ok {
				msg := fmt.Sprintf("[-] Address: %s | Error: failed to parse balance string", accBal.Account)
				fmt.Printf("%s\n", msg)
				appendLog(logFile, msg)
				continue
			}

			ethAmount := weiToEther(balanceWei)
			fmt.Printf("[+] Address: %s\n    Balance: %s Wei (~%.6f ETH)\n", accBal.Account, balanceWei.String(), ethAmount)

			if balanceWei.Cmp(big.NewInt(0)) > 0 {
				if _, err := fmt.Fprintf(outFile, "%s : %.6f ETH\n", accBal.Account, ethAmount); err != nil {
					msg := fmt.Sprintf("[-] Failed to append positive balance for %s: %v", accBal.Account, err)
					fmt.Printf("%s\n", msg)
					appendLog(logFile, msg)
				}
			}
		}
		fmt.Println()
	}
}

func ensureConfig() (bool, error) {
	if _, err := os.Stat(configFileName); errors.Is(err, os.ErrNotExist) {
		// Default config sets up Etherscan free tier speed limit (5 req/sec)
		defaultConfig := Config{
			APIKey:            "YOUR_ETHERSCAN_API_KEY_HERE",
			RequestsPerSecond: 5,
		}
		fileData, err := json.MarshalIndent(defaultConfig, "", "    ")
		if err != nil {
			return false, err
		}
		if err := os.WriteFile(configFileName, fileData, 0644); err != nil {
			return false, err
		}
		fmt.Printf("[+] Created default '%s'\n", configFileName)
		return true, nil
	}
	return false, nil
}

func ensureAddresses() (bool, error) {
	if _, err := os.Stat(addressesFileName); errors.Is(err, os.ErrNotExist) {
		defaultAddresses := []string{
			"0xd8dA6BF26964aF9D7eEd9e03E53415D37aA96045",
			"0xF977814e90dA44bFA03b6295A0616a897441aceC",
		}
		content := strings.Join(defaultAddresses, "\n") + "\n"
		if err := os.WriteFile(addressesFileName, []byte(content), 0644); err != nil {
			return false, err
		}
		fmt.Printf("[+] Created default '%s' with sample targets\n", addressesFileName)
		return true, nil
	}
	return false, nil
}

func readConfig(filename string) (*Config, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var cfg Config
	err = json.NewDecoder(file).Decode(&cfg)
	return &cfg, err
}

func readAddresses(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var addresses []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line != "" && !strings.HasPrefix(line, "#") {
			addresses = append(addresses, line)
		}
	}
	return addresses, scanner.Err()
}

func fetchMultiBalances(addresses []string, apiKey string) ([]AccountBalance, error) {
	commaSeparatedAddresses := strings.Join(addresses, ",")
	apiURL := fmt.Sprintf(
		"https://api.etherscan.io/v2/api?chainid=1&module=account&action=balancemulti&address=%s&tag=latest&apikey=%s",
		commaSeparatedAddresses, apiKey,
	)

	resp, err := http.Get(apiURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("http error: status code %d", resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var ethResp EtherscanMultiResponse
	if err := json.Unmarshal(body, &ethResp); err != nil {
		return nil, err
	}

	if ethResp.Status != "1" {
		errMsg := ethResp.Message
		if errMsg == "" || errMsg == "NOTOK" {
			errMsg = strings.Trim(string(ethResp.Result), "\n \t\r\"")
		}
		if errMsg == "" {
			errMsg = "unknown etherscan error"
		}
		return nil, errors.New(errMsg)
	}

	var balances []AccountBalance
	if err := json.Unmarshal(ethResp.Result, &balances); err != nil {
		return nil, err
	}

	return balances, nil
}

func isFatalAPIKeyError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "invalid api key") || strings.Contains(msg, "invalid apikey") || strings.Contains(msg, "api key") && strings.Contains(msg, "invalid")
}

func appendLog(logFile *os.File, message string) {
	timestamp := time.Now().Format("2006-01-02 15:04:05")
	logEntry := fmt.Sprintf("[%s] %s\n", timestamp, message)
	if _, err := logFile.WriteString(logEntry); err != nil {
		fmt.Printf("Failed to write to log: %v\n", err)
	}
}

func weiToEther(wei *big.Int) float64 {
	weiPerEth := new(big.Float).SetInt(new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil))
	balanceFloat := new(big.Float).SetInt(wei)
	etherFloat := new(big.Float).Quo(balanceFloat, weiPerEth)
	eth, _ := etherFloat.Float64()
	return eth
}
