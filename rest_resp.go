package dalgo2redis

import (
	"bytes"
	"strconv"
)

// The common readFrame guard has already capped and validated framing before
// this decoder sees any lengths. Never expose RESP error text to a caller.
func decodeRESTFrame(frame []byte) (any, error) {
	var parse func(int) (any, int, error)
	parse = func(offset int) (any, int, error) {
		end := offset + bytes.IndexByte(frame[offset:], '\n')
		header := frame[offset+1 : end-1]
		next := end + 1
		switch frame[offset] {
		case '+':
			return string(header), next, nil
		case '-':
			return nil, next, ErrRESTDenied
		case ':':
			n, err := strconv.ParseInt(string(header), 10, 64)
			if err != nil {
				return nil, next, ErrRESTProtocol
			}
			return n, next, nil
		case '$', '*':
			n, _ := strconv.ParseInt(string(header), 10, 64) // readFrame validated this length
			if n == -1 {
				return nil, next, nil
			}
			if frame[offset] == '$' {
				return string(frame[next : next+int(n)]), next + int(n) + 2, nil
			}
			values := make([]any, int(n))
			for i := range values {
				var err error
				values[i], next, err = parse(next)
				if err != nil {
					return nil, next, err
				}
			}
			return values, next, nil
		}
		return nil, next, ErrRESTProtocol
	}
	value, _, err := parse(0)
	return value, err
}
