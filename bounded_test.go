package dalgo2redis

import (
	"bufio"
	"errors"
	"strings"
	"testing"
)

func TestResponseBudgetBeforeAllocation(t *testing.T) {
	for _, frame := range []string{"$1073741824\r\n", "*1073741824\r\n", "$-2\r\n", strings.Repeat("*1\r\n", 18) + "$0\r\n\r\n", "$1048576\r\n"} {
		out := []byte{}
		if err := readFrame(bufio.NewReader(strings.NewReader(frame)), &out, 0); !errors.Is(err, errResponseBudget) {
			t.Fatalf("%q: %v", frame[:8], err)
		}
		if len(out) > 4096 {
			t.Fatalf("allocated excessive reply: %d", len(out))
		}
	}
	valid := "*3\r\n$5\r\nhello\r\n$-1\r\n:42\r\n"
	out := []byte{}
	if err := readFrame(bufio.NewReader(strings.NewReader(valid)), &out, 0); err != nil || string(out) != valid {
		t.Fatalf("%q %v", out, err)
	}
}
