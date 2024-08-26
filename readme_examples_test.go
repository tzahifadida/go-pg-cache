package gopgcache

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestGoPgCache(t *testing.T) {
	ctx := context.Background()

	postgres, db, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)
	defer db.Close()

	err = setupTestTable(db, "public", "users")
	require.NoError(t, err)

	config := CacheConfig{
		Schema:             "public",
		TableName:          "users",
		IDFieldName:        "ID",
		UpdatedAtFieldName: "UpdatedAt",
		ChannelName:        "cache_invalidation",
		MaxSize:            1000,
		Context:            ctx,
	}

	cache, err := NewCache[TestUser, uuid.UUID](db, config)
	require.NoError(t, err)
	defer cache.Shutdown()

	t.Run("CacheOperations", func(t *testing.T) {
		// Insert a user
		userID := uuid.New()
		_, err := db.ExecContext(ctx, `INSERT INTO "public"."users" (id, name) VALUES ($1, $2)`, userID, "John Doe")
		require.NoError(t, err)

		// Retrieve the user from the cache
		user, exists, err := cache.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "John Doe", user.Name)

		// Update the user's name in the DB
		_, err = db.ExecContext(ctx, `UPDATE "public"."users" SET name = $1 WHERE id = $2`, "Jane Doe", userID)
		require.NoError(t, err)

		// Invalidate the cache entry
		err = cache.NotifyRemove(userID)
		require.NoError(t, err)

		// Retrieve the user again (should fetch from DB due to out-of-sync)
		updatedUser, exists, err := cache.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Jane Doe", updatedUser.Name)
	})

	t.Run("NotifyRemoveMultiple", func(t *testing.T) {
		// Insert multiple users
		userIDs := make([]uuid.UUID, 3)
		for i := 0; i < 3; i++ {
			userIDs[i] = uuid.New()
			_, err := db.ExecContext(ctx, `INSERT INTO "public"."users" (id, name) VALUES ($1, $2)`, userIDs[i], fmt.Sprintf("User %d", i+1))
			require.NoError(t, err)
		}

		// Retrieve all users to cache them
		for _, id := range userIDs {
			_, exists, err := cache.Get(ctx, id)
			require.NoError(t, err)
			assert.True(t, exists)
		}

		// Notify removal of multiple users
		err = cache.NotifyRemove(userIDs...)
		require.NoError(t, err)

		// Retrieve users again (should fetch from DB)
		for i, id := range userIDs {
			user, exists, err := cache.Get(ctx, id)
			require.NoError(t, err)
			assert.True(t, exists)
			assert.Equal(t, fmt.Sprintf("User %d", i+1), user.Name)
		}
	})
}
