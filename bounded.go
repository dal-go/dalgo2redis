package dalgo2redis

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"net"
	"strconv"
)

// MaxResponseBytes bounds each complete RESP2 reply before the Redis driver sees
// any length headers. Checking after Do would allow attacker-controlled bulk
// lengths to allocate memory first.
const MaxResponseBytes = 1 << 20

var errResponseBudget = errors.New("Redis response exceeds safety budget")

type boundedConn struct {
	net.Conn
	reader  *bufio.Reader
	pending *bytes.Reader
}

func (c *boundedConn) Read(p []byte) (int, error) {
	if c.pending != nil && c.pending.Len() > 0 {
		return c.pending.Read(p)
	}
	frame := make([]byte, 0, 4096)
	if err := readFrame(c.reader, &frame, 0); err != nil {
		_ = c.Conn.Close()
		return 0, err
	}
	c.pending = bytes.NewReader(frame)
	return c.pending.Read(p)
}
func readFrame(r *bufio.Reader, out *[]byte, depth int) error {
	if depth > 16 {
		return errResponseBudget
	}
	header, err := r.ReadSlice('\n')
	if err != nil {
		return err
	}
	if len(header) < 3 || header[len(header)-2] != '\r' || len(header) > 4096 || len(*out)+len(header) > MaxResponseBytes {
		return errResponseBudget
	}
	*out = append(*out, header...)
	switch header[0] {
	case '+', '-', ':':
		return nil
	case '$', '*':
		n, err := strconv.ParseInt(string(header[1:len(header)-2]), 10, 64)
		if err != nil || n < -1 {
			return errResponseBudget
		}
		if n == -1 {
			return nil
		}
		if header[0] == '*' {
			if n > 256 {
				return errResponseBudget
			}
			for i := int64(0); i < n; i++ {
				if err = readFrame(r, out, depth+1); err != nil {
					return err
				}
			}
			return nil
		}
		if n > int64(MaxResponseBytes-len(*out)-2) {
			return errResponseBudget
		}
		start := len(*out)
		*out = append(*out, make([]byte, int(n)+2)...)
		if _, err = io.ReadFull(r, (*out)[start:]); err != nil {
			return err
		}
		if (*out)[len(*out)-2] != '\r' || (*out)[len(*out)-1] != '\n' {
			return errResponseBudget
		}
		return nil
	default:
		return errResponseBudget
	}
}
