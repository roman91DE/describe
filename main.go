package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type csvConfig struct {
	FilePath    string
	Separator   rune // Field delimiter
	HasHeader   bool // Whether to skip the first row
	MissingVals []string
}

// ParseCSVToMap parses a CSV file using the given config.
// Each column header should be a key in the returned map, with all values of that column as a slice.
func parseCSVToMap(config csvConfig) (map[string][]string, []string, error) {
	file, err := os.Open(config.FilePath)
	if err != nil {
		return nil, nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.Comma = config.Separator
	reader.TrimLeadingSpace = true

	var headers []string
	result := make(map[string][]string)

	// Read the first row to determine headers
	firstRow, err := reader.Read()
	if err == io.EOF {
		return result, headers, nil
	}
	if err != nil {
		return nil, nil, err
	}

	if config.HasHeader {
		headers = firstRow
	} else {
		headers = make([]string, len(firstRow))
		for i := range firstRow {
			headers[i] = "col" + strconv.Itoa(i+1)
		}
		// Reuse the first row as data
		for i, val := range firstRow {
			result[headers[i]] = []string{val}
		}
	}

	// Initialize empty slices for headers if not already filled
	for _, h := range headers {
		if _, ok := result[h]; !ok {
			result[h] = []string{}
		}
	}

	// Read and process the rest of the rows
	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, nil, err
		}

		for i, val := range record {
			if i < len(headers) {
				result[headers[i]] = append(result[headers[i]], val)
			}
		}
	}

	return result, headers, nil
}

func isMissing(val string, cfg csvConfig) bool {
	val = strings.TrimSpace(strings.ToLower(val))
	for _, m := range cfg.MissingVals {
		if val == strings.ToLower(m) {
			return true
		}
	}
	return false
}

type numericSummary struct {
	Name   string
	Min    float64
	Max    float64
	Mean   float64
	Median float64
	StdDev float64
	Q25    float64
	Q50    float64
	Q75    float64
}

type categoricalSummary struct {
	Name        string
	UniqueCount int
	Mode        string
	TopFrequent []string
	Frequencies map[string]int
}

func numericConverter(data map[string][]string, cfg csvConfig) (map[string][]string, map[string][]float64) {
	categorical := make(map[string][]string)
	numeric := make(map[string][]float64)

	for key, values := range data {
		var converted []float64
		var categoricalFallback []string
		allNumeric := true

		for _, v := range values {
			if isMissing(v, cfg) {
				categoricalFallback = append(categoricalFallback, "NA")
				continue
			}
			f, err := strconv.ParseFloat(v, 64)
			if err != nil {
				allNumeric = false
				break
			}
			converted = append(converted, f)
			categoricalFallback = append(categoricalFallback, v)
		}

		if allNumeric {
			numeric[key] = converted
		} else {
			categorical[key] = make([]string, len(values))
			for i, v := range values {
				if isMissing(v, cfg) {
					categorical[key][i] = "NA"
				} else {
					categorical[key][i] = v
				}
			}
		}
	}

	return categorical, numeric
}

func processNumericCol(name string, vals []float64) numericSummary {
	sort.Float64s(vals)

	min := vals[0]
	max := vals[len(vals)-1]

	var sum float64
	for _, v := range vals {
		sum += v
	}
	mean := sum / float64(len(vals))

	var variance float64
	for _, v := range vals {
		variance += math.Pow(v-mean, 2)
	}
	stddev := math.Sqrt(variance / float64(len(vals)))

	mid := len(vals) / 2
	var median float64
	if len(vals)%2 == 0 {
		median = (vals[mid-1] + vals[mid]) / 2
	} else {
		median = vals[mid]
	}

	q25 := vals[len(vals)*25/100]
	q50 := median
	q75 := vals[len(vals)*75/100]

	return numericSummary{
		Name:   name,
		Min:    min,
		Max:    max,
		Mean:   mean,
		Median: median,
		StdDev: stddev,
		Q25:    q25,
		Q50:    q50,
		Q75:    q75,
	}
}

func processCategoricalCol(name string, vals []string) categoricalSummary {
	counts := make(map[string]int)
	for _, v := range vals {
		counts[v]++
	}

	var mode string
	var maxCount int
	for val, count := range counts {
		if count > maxCount {
			mode = val
			maxCount = count
		}
	}

	// Sort values by frequency
	type kv struct {
		Key   string
		Value int
	}
	var sorted []kv
	for k, v := range counts {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Value > sorted[j].Value
	})

	topN := 3
	if len(sorted) < topN {
		topN = len(sorted)
	}
	topFrequent := make([]string, topN)
	for i := 0; i < topN; i++ {
		topFrequent[i] = sorted[i].Key
	}

	return categoricalSummary{
		Name:        name,
		UniqueCount: len(counts),
		Mode:        mode,
		TopFrequent: topFrequent,
		Frequencies: counts,
	}
}

func printNumericSummaryTable(summaries []numericSummary) {
	for _, s := range summaries {
		fmt.Printf("Column:   %s\n", s.Name)
		fmt.Printf("Mean:     %.2f\n", s.Mean)
		fmt.Printf("StdDev:   %.2f\n", s.StdDev)
		fmt.Printf("Min:      %.2f\n", s.Min)
		fmt.Printf("25%%:      %.2f\n", s.Q25)
		fmt.Printf("50%%:      %.2f\n", s.Q50)
		fmt.Printf("75%%:      %.2f\n", s.Q75)
		fmt.Printf("Max:      %.2f\n", s.Max)
		fmt.Println()
	}
}

func printCategoricalSummaryTable(summaries []categoricalSummary) {
	for _, s := range summaries {
		fmt.Printf("Column:       %s\n", s.Name)
		fmt.Printf("Uniques:      %d\n", s.UniqueCount)
		fmt.Printf("Mode:         %s\n", s.Mode)
		fmt.Printf("Top Frequent: %s\n", strings.Join(s.TopFrequent, ", "))
		fmt.Println()
	}
}

func main() {
	config := csvConfig{
		FilePath:    "test_data/titanic.csv",
		Separator:   ',',
		HasHeader:   true,
		MissingVals: []string{"", "na", "n/a", "null", "missing"},
	}

	data, _, err := parseCSVToMap(config)
	if err != nil {
		log.Fatal(err)
	}

	catCols, numCols := numericConverter(data, config)

	var (
		catSummaries []categoricalSummary
		numSummaries []numericSummary
	)

	catCh := make(chan categoricalSummary)
	numCh := make(chan numericSummary)

	var catWg sync.WaitGroup
	var numWg sync.WaitGroup

	// Categorical
	catWg.Add(len(catCols))
	for colName, colVals := range catCols {
		go func(name string, vals []string) {
			defer catWg.Done()
			catCh <- processCategoricalCol(name, vals)
		}(colName, colVals)
	}

	// Numeric
	numWg.Add(len(numCols))
	for colName, colVals := range numCols {
		go func(name string, vals []float64) {
			defer numWg.Done()
			numCh <- processNumericCol(name, vals)
		}(colName, colVals)
	}

	// Close channels once done
	go func() {
		catWg.Wait()
		close(catCh)
	}()

	go func() {
		numWg.Wait()
		close(numCh)
	}()

	for s := range catCh {
		catSummaries = append(catSummaries, s)
	}
	for s := range numCh {
		numSummaries = append(numSummaries, s)
	}

	printCategoricalSummaryTable(catSummaries)
	printNumericSummaryTable(numSummaries)

}
