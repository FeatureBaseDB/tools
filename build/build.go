package build

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/exec"
)

func Binary(pkg, goos, goarch string) (io.Reader, error) {
	binFile, err := ioutil.TempFile("", "pi")
	if err != nil {
		return nil, fmt.Errorf("creating temp file: %v", err)
	}
	com := exec.Command("go", "build", "-o", binFile.Name(), pkg)
	com.Env = append([]string{"GOOS=" + goos, "GOARCH=" + goarch}, os.Environ()...)
	stderr, err := com.StderrPipe()
	if err != nil {
		return nil, fmt.Errorf("getting stderr pipe from build command: %v", err)
	}

	err = com.Start()
	if err != nil {
		return nil, fmt.Errorf("starting go build command: %v", err)
	}

	outputErr, err := ioutil.ReadAll(stderr)
	if err != nil {
		return nil, fmt.Errorf("reading build command stderr: %v", err)
	}

	err = com.Wait()
	if err != nil {
		return nil, fmt.Errorf("build command error: %v, output: %s", err, outputErr)
	}

	f, err := os.Open(binFile.Name())
	if err != nil {
		return nil, fmt.Errorf("opening built binary: %v", err)
	}
	return f, nil
}
