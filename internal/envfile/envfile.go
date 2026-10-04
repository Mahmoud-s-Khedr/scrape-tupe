package envfile

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

// Load reads a .env file and sets any variables that are not already set
// in the environment. Existing environment variables take precedence.
// Lines starting with # and empty lines are ignored.
// Returns nil if the file does not exist.
func Load(path string) error {
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil // .env is optional
		}
		return fmt.Errorf("open %s: %w", path, err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())

		// Skip empty lines and comments
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}

		// Split on first '='
		parts := strings.SplitN(line, "=", 2)
		if len(parts) != 2 {
			continue
		}

		key := strings.TrimSpace(parts[0])
		value := strings.TrimSpace(parts[1])

		// Remove surrounding quotes if present
		if len(value) >= 2 {
			if (value[0] == '"' && value[len(value)-1] == '"') ||
				(value[0] == '\'' && value[len(value)-1] == '\'') {
				value = value[1 : len(value)-1]
			}
		}

		// Don't override existing environment variables. LookupEnv matters here:
		// an explicitly empty process value is still an intentional override.
		if _, ok := os.LookupEnv(key); !ok {
			os.Setenv(key, value)
		}
	}

	return scanner.Err()
}
