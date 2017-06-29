package bench

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"context"
)

func getClusterVersion(ctx context.Context, hosts []string) (string, error) {
	if len(hosts) == 0 {
		return "", fmt.Errorf("bagent can't get cluster version with no pilosa hosts configured")
	}
	resp, err := http.Get("http://" + hosts[0] + "/version")
	if err != nil {
		return "", fmt.Errorf("getClusterVersion http.Get: %v", err)
	}
	dec := json.NewDecoder(resp.Body)
	v := struct{ Version string }{}
	err = dec.Decode(&v)
	if err != nil {
		return "", fmt.Errorf("getClusterVersion decoding: %v", err)
	}
	return v.Version, nil
}

func RunBenchmark(ctx context.Context, hosts []string, agentNum int, b Benchmark) (result map[string]interface{}) {
	result = make(map[string]interface{})
	version, err := getClusterVersion(ctx, hosts)
	if err != nil {
		result["error"] = err.Error()
		return result
	}

	err = b.Init(hosts, agentNum)
	if err != nil {
		result["error"] = err.Error()
		return result
	}
	start := time.Now()
	result["output"] = b.Run(context.Background())
	result["duration"] = time.Since(start)
	result["agent-num"] = agentNum
	result["pilosa-version"] = version
	result["configuration"] = b
	return result
}
