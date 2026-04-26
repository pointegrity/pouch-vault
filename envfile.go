package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// loadEnvFile reads a KEY=VALUE file (the format systemd's
// EnvironmentFile= and shell `source` both accept) and sets the
// values into the process env. Existing env values are NOT
// overridden — explicit env / CLI flags always win over the file.
//
// Comment lines (#) and blank lines are skipped. Leading "export "
// is tolerated. Quotes around values are stripped if they wrap the
// whole value.
//
// Errors only on read failure; missing file is a soft no-op so a
// daemon can declare an optional config path without bombing when
// it doesn't exist.
func loadEnvFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		line = strings.TrimPrefix(line, "export ")
		eq := strings.IndexByte(line, '=')
		if eq <= 0 {
			return fmt.Errorf("%s:%d: expected KEY=VALUE", path, lineNum)
		}
		key := strings.TrimSpace(line[:eq])
		val := strings.TrimSpace(line[eq+1:])
		// Strip surrounding quotes if the value is quoted.
		if len(val) >= 2 {
			if (val[0] == '"' && val[len(val)-1] == '"') ||
				(val[0] == '\'' && val[len(val)-1] == '\'') {
				val = val[1 : len(val)-1]
			}
		}
		// Don't override anything already in the environment — env
		// + CLI must beat the file.
		if _, present := os.LookupEnv(key); present {
			continue
		}
		_ = os.Setenv(key, val)
	}
	return scanner.Err()
}
