package gopgcache

import (
	"context"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

func TestReadmeExamples(t *testing.T) {
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

	t.Run("BasicUsage", func(t *testing.T) {
		userID := uuid.New()
		_, err := db.ExecContext(ctx, `INSERT INTO "public"."users" (id, name) VALUES ($1, $2)`, userID, "John Doe")
		require.NoError(t, err)

		user, exists, err := cache.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "John Doe", user.Name)

		err = cache.NotifyRemove(userID, uuid.New(), uuid.New())
		require.NoError(t, err)
	})

	t.Run("RetrievingAnItem", func(t *testing.T) {
		userID := uuid.New()
		_, err := db.ExecContext(ctx, `INSERT INTO "public"."users" (id, name) VALUES ($1, $2)`, userID, "Jane Doe")
		require.NoError(t, err)

		user, exists, err := cache.Get(ctx, userID)
		require.NoError(t, err)
		if exists {
			assert.Equal(t, "Jane Doe", user.Name)
		} else {
			t.Log("User not found")
		}
	})

	t.Run("RetrievingAnItemCacheOnly", func(t *testing.T) {
		userID := uuid.New()
		_, err := db.ExecContext(ctx, `INSERT INTO "public"."users" (id, name) VALUES ($1, $2)`, userID, "Bob Smith")
		require.NoError(t, err)

		// First, get the user to cache it
		_, _, err = cache.Get(ctx, userID)
		require.NoError(t, err)

		// Now retrieve from cache only
		user, exists, err := cache.Get(ctx, userID, CacheOnly())
		require.NoError(t, err)
		if exists {
			assert.Equal(t, "Bob Smith", user.Name)
		} else {
			t.Log("User not found in cache")
		}
	})

	t.Run("InvalidatingMultipleCacheEntries", func(t *testing.T) {
		userID1 := uuid.New()
		userID2 := uuid.New()
		userID3 := uuid.New()

		_, err := db.ExecContext(ctx, `INSERT INTO "public"."users" (id, name) VALUES ($1, $2), ($3, $4), ($5, $6)`,
			userID1, "User1", userID2, "User2", userID3, "User3")
		require.NoError(t, err)

		// Cache the users
		for _, id := range []uuid.UUID{userID1, userID2, userID3} {
			_, _, err = cache.Get(ctx, id)
			require.NoError(t, err)
		}

		err = cache.NotifyRemove(userID1, userID2, userID3)
		require.NoError(t, err)

		// Verify that the entries are removed from cache
		for _, id := range []uuid.UUID{userID1, userID2, userID3} {
			_, exists, err := cache.Get(ctx, id, CacheOnly())
			require.NoError(t, err)
			assert.False(t, exists)
		}
	})

	t.Run("InvalidatingCacheEntriesInATransaction", func(t *testing.T) {
		userID1 := uuid.New()
		userID2 := uuid.New()
		userID3 := uuid.New()

		_, err := db.ExecContext(ctx, `INSERT INTO "public"."users" (id, name) VALUES ($1, $2), ($3, $4), ($5, $6)`,
			userID1, "TxUser1", userID2, "TxUser2", userID3, "TxUser3")
		require.NoError(t, err)

		// Cache the users
		for _, id := range []uuid.UUID{userID1, userID2, userID3} {
			_, _, err = cache.Get(ctx, id)
			require.NoError(t, err)
		}

		notifyQueryResult, err := cache.NotifyRemoveAndGetQuery(userID1, userID2, userID3)
		require.NoError(t, err)

		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		_, err = tx.ExecContext(ctx, notifyQueryResult.Query, notifyQueryResult.Params...)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
		// Verify that the entries are removed from cache
		for _, id := range []uuid.UUID{userID1, userID2, userID3} {
			_, exists, err := cache.Get(ctx, id, CacheOnly())
			require.NoError(t, err)
			assert.False(t, exists)
		}
	})

	t.Run("UsingNotifyRemoveAndGetQueryForFlexibleCacheInvalidation", func(t *testing.T) {
		userID1 := uuid.New()
		userID2 := uuid.New()
		userID3 := uuid.New()

		// Insert test users
		_, err := db.ExecContext(ctx, `INSERT INTO "public"."users" (id, name) VALUES ($1, $2), ($3, $4), ($5, $6)`,
			userID1, "FlexUser1", userID2, "FlexUser2", userID3, "FlexUser3")
		require.NoError(t, err)

		// Cache the users
		for _, id := range []uuid.UUID{userID1, userID2, userID3} {
			_, _, err = cache.Get(ctx, id)
			require.NoError(t, err)
		}

		// Get the notify query for cache invalidation
		notifyQueryResult, err := cache.NotifyRemoveAndGetQuery(userID1, userID2, userID3)
		require.NoError(t, err)

		// Use the query in a transaction
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		// Simulate some database operations
		_, err = tx.ExecContext(ctx, `UPDATE "public"."users" SET name = $1 WHERE id IN ($2, $3, $4)`,
			"UpdatedUser", userID1, userID2, userID3)
		require.NoError(t, err)

		// Execute the notify query
		_, err = tx.ExecContext(ctx, notifyQueryResult.Query, notifyQueryResult.Params...)
		require.NoError(t, err)

		// Commit the transaction
		err = tx.Commit()
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
		// Verify that the entries are removed from cache
		for _, id := range []uuid.UUID{userID1, userID2, userID3} {
			_, exists, err := cache.Get(ctx, id, CacheOnly())
			require.NoError(t, err)
			assert.False(t, exists, "User should not be in cache after invalidation")
		}

		// Verify that the database has been updated
		for _, id := range []uuid.UUID{userID1, userID2, userID3} {
			var user TestUser
			err = db.GetContext(ctx, &user, `SELECT * FROM "public"."users" WHERE id = $1`, id)
			require.NoError(t, err)
			assert.Equal(t, "UpdatedUser", user.Name, "User name should be updated in the database")
		}
	})

	t.Run("DirectlyAddingOrUpdatingAnItemInCache", func(t *testing.T) {
		user := TestUser{
			ID:        uuid.New(),
			Name:      "John Doe",
			UpdatedAt: time.Now(),
		}
		cache.Put(ctx, user.ID, user)

		// Verify the user is in the cache
		cachedUser, exists, err := cache.Get(ctx, user.ID, CacheOnly())
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, user, cachedUser)
	})
}
