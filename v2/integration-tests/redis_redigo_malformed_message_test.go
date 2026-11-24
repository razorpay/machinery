package integration_test

import (
	"os"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/RichardKnop/machinery/v2"
	redisbackend "github.com/RichardKnop/machinery/v2/backends/redis"
	redisbroker "github.com/RichardKnop/machinery/v2/brokers/redis"
	"github.com/RichardKnop/machinery/v2/config"
	eagerlock "github.com/RichardKnop/machinery/v2/locks/eager"
)

// A malformed message in the queue makes the redigo broker's consume() return an
// error. The worker must restart consuming and still run the next valid task, as
// the v1 broker does. Before the fix it popped and re-pushed messages forever
// without running them.
func TestRedisRedigo_MalformedMessageDoesNotStopConsuming(t *testing.T) {
	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		t.Skip("REDIS_URL is not defined")
	}

	cnf := &config.Config{
		DefaultQueue:    "machinery_malformed_message_test",
		ResultsExpireIn: 3600,
		Redis: &config.RedisConfig{
			MaxIdle:                3,
			IdleTimeout:            240,
			ReadTimeout:            15,
			WriteTimeout:           15,
			ConnectTimeout:         15,
			NormalTasksPollPeriod:  1000,
			DelayedTasksPollPeriod: 500,
		},
	}

	conn, err := redis.Dial("tcp", redisURL)
	require.NoError(t, err)
	defer conn.Close()
	_, err = conn.Do("DEL", cnf.DefaultQueue)
	require.NoError(t, err)
	_, err = conn.Do("RPUSH", cnf.DefaultQueue, "this is not a task signature")
	require.NoError(t, err)

	broker := redisbroker.New(cnf, redisURL, "", "", "", 0)
	backend := redisbackend.New(cnf, redisURL, "", "", "", 0)
	server := machinery.NewServer(cnf, broker, backend, eagerlock.New())
	registerTestTasks(server)

	worker := server.NewWorker("malformed_message_worker", 1)
	go worker.Launch()
	defer worker.Quit()

	asyncResult, err := server.SendTask(newAddTask(1, 2))
	require.NoError(t, err)
	results, err := asyncResult.GetWithTimeout(30*time.Second, 100*time.Millisecond)
	require.NoError(t, err, "the valid task after the malformed message never ran")
	require.Len(t, results, 1)
	assert.Equal(t, int64(3), results[0].Interface())
}
