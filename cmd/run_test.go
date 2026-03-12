package cmd

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRunCmd(t *testing.T) {
	rootCmd.SetArgs([]string{"run", "../testdata/not-exists.yaml"})
	err := rootCmd.Execute()
	if runtime.GOOS == "windows" {
		assert.Contains(t, err.Error(), "The system cannot find the file specified")
	} else {
		assert.Contains(t, err.Error(), "no such file or directory")
	}
}
