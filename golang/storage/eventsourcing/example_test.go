package eventsourcing

import (
	"context"
	"fmt"
)

func ExampleEventStreamStore() {
	// Production callers supply a provider-backed kv.KeyValueStore here.
	table := newMemoryKV()
	ctx := context.Background()
	store, _ := NewEventStreamStore(table, func() []string { return nil },
		func(_ context.Context, state []string, changes []EventChange) ([]string, error) {
			for _, change := range changes {
				state = append(state, fmt.Sprintf("v%d:%s", change.Version, change.Data))
			}
			return state, nil
		})
	store.TryAppend(ctx, "stream", 0, appendNumbers("first-operation", 1, 10, 20))
	store.TryAppend(ctx, "stream", 1, appendNumbers("second-operation", 2, 30))
	state, _ := store.ReadState(ctx, "stream")
	fmt.Println(state.Revision, state.Value)
	// Output: 2 [v1:10 v1:20 v2:30]
}
