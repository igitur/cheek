package cmd

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunCmd(t *testing.T) {
	// wrong fn will stop the scheduler from going live
	rootCmd.SetArgs([]string{"run", "../testdata/not-exists.yaml"})
	err := rootCmd.Execute()
	assert.Error(t, err)
	expected := getExpectedErrorString()
	assert.Contains(t, err.Error(), expected)
}

func getExpectedErrorString() string {
	if runtime.GOOS == "windows" {
		return "The system cannot find the file specified."
	}
	return "no such file or directory"
}
