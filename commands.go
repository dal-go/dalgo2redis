package dalgo2redis

import (
	"errors"
	"strconv"
	"strings"
)

// Options is trusted client configuration, never an untrusted command payload.
type Options struct {
	// ScanPrefix enables exactly one SCAN page under this literal byte prefix.
	// Empty disables SCAN. Redis ACL patterns alone do not scope enumeration.
	ScanPrefix string
}

func validateOptions(options Options) error {
	if len(options.ScanPrefix) > 65520 {
		return errors.New("Redis scan prefix exceeds command budget")
	}
	return nil
}

// ValidateCommandWithOptions validates before opening a connection. SCAN is
// refused unless options supply a literal prefix; Query repeats this admission.
func ValidateCommandWithOptions(args []string, options Options) error {
	_, err := prepareCommand(args, options)
	return err
}

func prepareCommand(args []string, options Options) ([]string, error) {
	if err := validateOptions(options); err != nil {
		return nil, err
	}
	if err := validateRead(args); err != nil {
		return nil, err
	}
	admitted := append([]string(nil), args...)
	admitted[0] = strings.ToUpper(admitted[0])
	if admitted[0] != "SCAN" {
		return admitted, nil
	}
	if options.ScanPrefix == "" {
		return nil, errors.New("Redis SCAN requires a configured literal prefix")
	}
	var pattern strings.Builder
	// Byte iteration retains arbitrary prefix bytes; only ASCII glob syntax
	// needs quoting. A rune conversion would corrupt invalid UTF-8 prefixes.
	for i := 0; i < len(options.ScanPrefix); i++ {
		b := options.ScanPrefix[i]
		if strings.ContainsRune("\\*?[]", rune(b)) {
			pattern.WriteByte('\\')
		}
		pattern.WriteByte(b)
	}
	pattern.WriteByte('*')
	expected := pattern.String()
	matched, counted := false, false
	for i := 2; i < len(admitted); i += 2 {
		admitted[i] = strings.ToUpper(admitted[i])
		switch admitted[i] {
		case "MATCH":
			if admitted[i+1] != expected {
				return nil, errors.New("Redis SCAN MATCH conflicts with configured scope")
			}
			matched = true
		case "COUNT":
			counted = true
		}
	}
	if !matched {
		admitted = append(admitted, "MATCH", expected)
	}
	if !counted {
		admitted = append(admitted, "COUNT", "100")
	}
	if err := validateRead(admitted); err != nil {
		return nil, err
	}
	return admitted, nil
}

func decimalUint64(value string) bool {
	if value == "" || len(value) > 20 {
		return false
	}
	for i := 0; i < len(value); i++ {
		if value[i] < '0' || value[i] > '9' {
			return false
		}
	}
	_, err := strconv.ParseUint(value, 10, 64)
	return err == nil
}

func validateScan(args []string) error {
	if !decimalUint64(args[1]) || len(args)%2 != 0 {
		return errors.New("invalid Redis SCAN cursor or options")
	}
	seen := make(map[string]bool)
	for i := 2; i < len(args); i += 2 {
		option, value := strings.ToUpper(args[i]), args[i+1]
		if seen[option] {
			return errors.New("duplicate Redis SCAN option")
		}
		seen[option] = true
		switch option {
		case "MATCH", "TYPE":
			if value == "" {
				return errors.New("empty Redis SCAN option")
			}
		case "COUNT":
			n, err := strconv.ParseUint(value, 10, 64)
			if !decimalUint64(value) || err != nil || n < 1 || n > 1000 {
				return errors.New("Redis SCAN COUNT must be between 1 and 1000")
			}
		default:
			return errors.New("unsupported Redis SCAN option")
		}
	}
	return nil
}

// A matching request is insufficient if the service returns a malformed or
// out-of-scope page. Refuse the entire page, never silently filter or truncate.
func validateScanReply(value any, prefix string) error {
	bad := errors.New("invalid or out-of-scope Redis SCAN response")
	page, ok := value.([]any)
	if !ok || len(page) != 2 {
		return bad
	}
	cursor, ok := page[0].(string)
	if !ok || !decimalUint64(cursor) {
		return bad
	}
	keys, ok := page[1].([]any)
	if !ok {
		return bad
	}
	for _, key := range keys {
		text, ok := key.(string)
		if !ok || !strings.HasPrefix(text, prefix) {
			return bad
		}
	}
	return nil
}
