package logging

import "os"

// Small env helpers isolated so the main logging_test.go stays focused
// on the logging assertions. Wraps os so the test file doesn't grow
// os-import noise.

func lookupEnv(k string) (string, bool) { return os.LookupEnv(k) }
func setEnv(k, v string) error          { return os.Setenv(k, v) }
func unsetEnv(k string) error           { return os.Unsetenv(k) }
