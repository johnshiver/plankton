package task

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunnerSerialization(t *testing.T) {

	var (
		t1, t2 *TestTaskRunner
		r1, r2 Runner
	)

	t1 = &TestTaskRunner{
		N:    1,
		X:    "here-it-is",
		Z:    2,
		Data: make(map[string]int),
	}
	t2 = &TestTaskRunner{
		Data: make(map[string]int),
	}

	r1 = t1
	r2 = t2

	bytes, err := json.Marshal(r1)
	require.Nil(t, err)
	require.Equal(t, []byte(`{"N":1,"X":"here-it-is","Z":2}`), bytes)

	err = r1.Run(context.Background())
	require.Nil(t, err)

	err = json.Unmarshal(bytes, r2)
	require.Nil(t, err)
	err = r2.Run(context.Background())
	require.Nil(t, err)

	require.Equal(t, t1, t2)
}
