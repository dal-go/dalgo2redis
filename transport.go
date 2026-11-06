package dalgo2redis

import (
	"context"
	"errors"
	"sync"

	"github.com/redis/go-redis/v9"
)

// go-redis deadlines alone do not interrupt an in-flight socket read when a
// caller cancels without advancing its deadline. A per-read driver lets us close
// exactly the operation's connections; the shared client's pool is never closed
// by a caller abort. Admission retains the original configured connection limit.
type readOwner struct {
	mu      sync.Mutex
	wg      sync.WaitGroup
	options redis.Options
	closed  bool
	done    chan struct{}
	slots   chan struct{}
	active  map[*redis.Client]context.CancelFunc
}

func newReadOwner(options *redis.Options) *readOwner {
	limit := options.PoolSize
	if options.MaxActiveConns > 0 && options.MaxActiveConns < limit {
		limit = options.MaxActiveConns
	}
	if limit < 1 {
		limit = 1
	}
	return &readOwner{
		options: *options,
		done:    make(chan struct{}),
		slots:   make(chan struct{}, limit),
		active:  make(map[*redis.Client]context.CancelFunc),
	}
}

func (o *readOwner) acquire(ctx context.Context, cancel context.CancelFunc) (*redis.Client, func(), error) {
	select {
	case o.slots <- struct{}{}:
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-o.done:
		return nil, nil, errors.New("Redis client is closed")
	}
	o.mu.Lock()
	if o.closed || ctx.Err() != nil {
		o.mu.Unlock()
		<-o.slots
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		return nil, nil, errors.New("Redis client is closed")
	}
	options := o.options
	options.PoolSize = 1
	options.MaxActiveConns = 1
	options.MinIdleConns = 0
	options.MaxIdleConns = 1
	driver := redis.NewClient(&options)
	o.active[driver] = cancel
	o.wg.Add(1)
	o.mu.Unlock()

	finished := make(chan struct{})
	stop := context.AfterFunc(ctx, func() {
		_ = driver.Close()
		close(finished)
	})
	var once sync.Once
	release := func() {
		once.Do(func() {
			if !stop() {
				<-finished
			}
			_ = driver.Close()
			o.mu.Lock()
			delete(o.active, driver)
			o.mu.Unlock()
			<-o.slots
			o.wg.Done()
		})
	}
	return driver, release, nil
}

func (o *readOwner) close() {
	o.mu.Lock()
	if o.closed {
		o.mu.Unlock()
		o.wg.Wait()
		return
	}
	o.closed = true
	close(o.done)
	active := make(map[*redis.Client]context.CancelFunc, len(o.active))
	for driver, cancel := range o.active {
		active[driver] = cancel
	}
	o.mu.Unlock()
	for driver, cancel := range active {
		cancel()
		_ = driver.Close()
	}
	o.wg.Wait()
}
