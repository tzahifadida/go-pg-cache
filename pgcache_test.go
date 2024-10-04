package gopgcache

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	"github.com/google/uuid"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type TestUser struct {
	ID        uuid.UUID `db:"id"`
	Name      string    `db:"name"`
	UpdatedAt time.Time `db:"updated_at"`
}

func startPostgresContainer(ctx context.Context) (testcontainers.Container, *sqlx.DB, error) {
	req := testcontainers.ContainerRequest{
		Image:        "postgres:13",
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "testuser",
			"POSTGRES_PASSWORD": "testpass",
			"POSTGRES_DB":       "testdb",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections").WithStartupTimeout(60 * time.Second),
	}

	postgres, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("failed to start postgres container: %v", err)
	}

	host, err := postgres.Host(ctx)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get postgres host: %v", err)
	}

	port, err := postgres.MappedPort(ctx, "5432")
	if err != nil {
		return nil, nil, fmt.Errorf("failed to get postgres port: %v", err)
	}

	dsn := fmt.Sprintf("host=%s port=%d user=testuser password=testpass dbname=testdb sslmode=disable", host, port.Int())

	var db *sqlx.DB
	err = retry(ctx, 30*time.Second, func() error {
		var err error
		db, err = sqlx.Connect("pgx", dsn)
		if err != nil {
			return err
		}
		return db.Ping()
	})

	if err != nil {
		return nil, nil, fmt.Errorf("failed to connect to database after retries: %v", err)
	}

	return postgres, db, nil
}

func retry(ctx context.Context, maxWait time.Duration, fn func() error) error {
	start := time.Now()
	for {
		err := fn()
		if err == nil {
			return nil
		}

		if time.Since(start) > maxWait {
			return fmt.Errorf("timeout after %v: %w", maxWait, err)
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(1 * time.Second):
			log.Printf("Retrying after error: %v", err)
		}
	}
}

func setupTestTable(db *sqlx.DB, schema, table string) error {
	_, err := db.Exec(fmt.Sprintf(`
		CREATE SCHEMA IF NOT EXISTS "%s";
		CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
		CREATE TABLE "%s"."%s" (
			id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
			name TEXT NOT NULL,
			updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		)
	`, schema, schema, table))
	return err
}

func TestGoPGCacheWithRealDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	postgres, db, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)
	defer db.Close()

	schema := "test_schema"
	table := "users"
	err = setupTestTable(db, schema, table)
	require.NoError(t, err)

	config := CacheConfig{
		Schema:             schema,
		TableName:          table,
		IDFieldName:        "ID",
		UpdatedAtFieldName: "UpdatedAt",
		ChannelName:        "user_cache_updates",
		MaxSize:            100,
		Context:            ctx,
	}

	cache, err := NewCache[TestUser, uuid.UUID](db, config)
	require.NoError(t, err)
	defer cache.Shutdown()

	t.Run("InsertAndRetrieve", func(t *testing.T) {
		// Insert a user
		userID := uuid.New()
		_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (id, name) VALUES ($1, $2)`, schema, table), userID, "John Doe")
		require.NoError(t, err)

		// Retrieve the user through the cache
		user, exists, err := cache.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "John Doe", user.Name)

		// Retrieve again (should be from cache)
		cachedUser, exists, err := cache.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, user, cachedUser)
	})

	t.Run("UpdateAndRetrieve", func(t *testing.T) {
		userID := uuid.New()
		_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (id, name) VALUES ($1, $2)`, schema, table), userID, "Jane Doe")
		require.NoError(t, err)

		// Update the user
		_, err = db.ExecContext(ctx, fmt.Sprintf(`UPDATE "%s"."%s" SET name = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`, schema, table), "Jane Smith", userID)
		require.NoError(t, err)

		// Notify cache to remove the updated user
		err = cache.NotifyRemove(userID)
		require.NoError(t, err)

		// Retrieve the updated user
		updatedUser, exists, err := cache.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Jane Smith", updatedUser.Name)
	})

	t.Run("DeleteAndRetrieve", func(t *testing.T) {
		userID := uuid.New()
		_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (id, name) VALUES ($1, $2)`, schema, table), userID, "Alice")
		require.NoError(t, err)

		// Delete the user
		_, err = db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM "%s"."%s" WHERE id = $1`, schema, table), userID)
		require.NoError(t, err)

		// Notify cache to remove the deleted user
		err = cache.NotifyRemove(userID)
		require.NoError(t, err)

		// Try to retrieve the deleted user
		_, exists, err := cache.Get(ctx, userID)
		assert.ErrorIs(t, err, sql.ErrNoRows)
		assert.False(t, exists)
	})

	t.Run("CacheEviction", func(t *testing.T) {
		// Create a new cache with a small size
		smallCache, err := NewCache[TestUser, uuid.UUID](db, CacheConfig{
			Schema:             schema,
			TableName:          table,
			IDFieldName:        "ID",
			UpdatedAtFieldName: "UpdatedAt",
			ChannelName:        "user_cache_updates",
			MaxSize:            2,
			Context:            ctx,
		})
		require.NoError(t, err)
		defer smallCache.Shutdown()

		// Insert three users
		userIDs := make([]uuid.UUID, 3)
		for i := 0; i < 3; i++ {
			userIDs[i] = uuid.New()
			_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (id, name) VALUES ($1, $2)`, schema, table), userIDs[i], fmt.Sprintf("User %d", i+1))
			require.NoError(t, err)
		}

		// Retrieve all three users
		for _, id := range userIDs {
			_, exists, err := smallCache.Get(ctx, id)
			require.NoError(t, err)
			assert.True(t, exists)
		}

		// Check that only the last two users are in the cache
		smallCache.mutex.RLock()
		assert.Equal(t, 2, len(smallCache.cache))
		_, exists := smallCache.cache[userIDs[0]]
		assert.False(t, exists)
		_, exists = smallCache.cache[userIDs[1]]
		assert.True(t, exists)
		_, exists = smallCache.cache[userIDs[2]]
		assert.True(t, exists)
		smallCache.mutex.RUnlock()
	})

	t.Run("RefreshCache", func(t *testing.T) {
		// Insert a new user
		userID := uuid.New()
		_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (id, name) VALUES ($1, $2)`, schema, table), userID, "Refresh Test")
		require.NoError(t, err)

		// Retrieve the user to cache it
		user, exists, err := cache.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Refresh Test", user.Name)

		// Update the user directly in the database
		_, err = db.ExecContext(ctx, fmt.Sprintf(`UPDATE "%s"."%s" SET name = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`, schema, table), "Refreshed User", userID)
		require.NoError(t, err)

		// Manually trigger a cache refresh
		err = cache.refreshCache(ctx)
		require.NoError(t, err)

		// Retrieve the user again
		refreshedUser, exists, err := cache.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Refreshed User", refreshedUser.Name)
	})

	// New test for NotifyRemoveAndGetQuery
	t.Run("NotifyRemoveAndGetQuery", func(t *testing.T) {
		userID := uuid.New()
		_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (id, name) VALUES ($1, $2)`, schema, table), userID, "Test User")
		require.NoError(t, err)

		// Retrieve the user to cache it
		user, exists, err := cache.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Test User", user.Name)

		// Get the notify query
		notifyQueryResult, err := cache.NotifyRemoveAndGetQuery(userID)
		require.NoError(t, err)

		// Execute the notify query
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		_, err = tx.ExecContext(ctx, notifyQueryResult.Query, notifyQueryResult.Params...)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
		// Check that the user is removed from the cache
		cache.mutex.RLock()
		_, exists = cache.cache[userID]
		cache.mutex.RUnlock()
		assert.False(t, exists)
	})

	// New test for Put
	t.Run("Put", func(t *testing.T) {
		userID := uuid.New()
		user := TestUser{
			ID:        userID,
			Name:      "Put Test User",
			UpdatedAt: time.Now(),
		}

		// Put the user in the cache
		cache.Put(ctx, userID, user)

		// Retrieve the user from the cache
		cachedUser, exists, err := cache.Get(ctx, userID, CacheOnly())
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, user, cachedUser)

		// Verify the user is not in the database
		var dbUser TestUser
		err = db.GetContext(ctx, &dbUser, fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE id = $1`, schema, table), userID)
		assert.Error(t, err) // Expect an error because the user should not be in the database
	})

	// New test for Get with CacheOnly option
	t.Run("GetCacheOnly", func(t *testing.T) {
		userID := uuid.New()
		_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (id, name) VALUES ($1, $2)`, schema, table), userID, "Cache Only Test")
		require.NoError(t, err)

		// Try to get the user with CacheOnly (should not exist in cache)
		user, exists, err := cache.Get(ctx, userID, CacheOnly())
		require.NoError(t, err)
		assert.False(t, exists)
		assert.Equal(t, TestUser{}, user)

		// Get the user normally (should load from DB and cache it)
		user, exists, err = cache.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Cache Only Test", user.Name)

		// Now try CacheOnly again (should exist in cache)
		cachedUser, exists, err := cache.Get(ctx, userID, CacheOnly())
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, user, cachedUser)
	})
}

func TestMultipleCachesWithRealDatabase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	postgres, db, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)
	defer db.Close()

	schema := "test_schema"
	table := "users"
	err = setupTestTable(db, schema, table)
	require.NoError(t, err)

	config := CacheConfig{
		Schema:             schema,
		TableName:          table,
		IDFieldName:        "ID",
		UpdatedAtFieldName: "UpdatedAt",
		ChannelName:        "user_cache_updates",
		MaxSize:            100,
		Context:            ctx,
	}

	cache1, err := NewCache[TestUser, uuid.UUID](db, config)
	require.NoError(t, err)
	defer cache1.Shutdown()

	cache2, err := NewCache[TestUser, uuid.UUID](db, config)
	require.NoError(t, err)
	defer cache2.Shutdown()

	t.Run("NotificationBetweenCaches", func(t *testing.T) {
		// Insert a user
		userID := uuid.New()
		_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (id, name) VALUES ($1, $2)`, schema, table), userID, "Multi Cache Test")
		require.NoError(t, err)

		// Retrieve the user in both caches
		user1, exists, err := cache1.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Multi Cache Test", user1.Name)

		user2, exists, err := cache2.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Multi Cache Test", user2.Name)

		// Update the user
		_, err = db.ExecContext(ctx, fmt.Sprintf(`UPDATE "%s"."%s" SET name = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`, schema, table), "Updated Multi Cache", userID)
		require.NoError(t, err)

		// Notify removal from cache1
		err = cache1.NotifyRemove(userID)
		require.NoError(t, err)

		// Wait a bit for the notification to propagate
		time.Sleep(100 * time.Millisecond)

		// Check that the user is removed from both caches
		cache1.mutex.RLock()
		_, exists = cache1.cache[userID]
		cache1.mutex.RUnlock()
		assert.False(t, exists)

		cache2.mutex.RLock()
		_, exists = cache2.cache[userID]
		cache2.mutex.RUnlock()
		assert.False(t, exists)

		// Retrieve the updated user in both caches
		updatedUser1, exists, err := cache1.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Updated Multi Cache", updatedUser1.Name)

		updatedUser2, exists, err := cache2.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Updated Multi Cache", updatedUser2.Name)
	})

	// New test for Put and Get with CacheOnly across multiple caches
	t.Run("PutAndGetCacheOnlyAcrossMultipleCaches", func(t *testing.T) {
		userID := uuid.New()
		user := TestUser{
			ID:        userID,
			Name:      "Multi Cache Put Test",
			UpdatedAt: time.Now(),
		}

		// Put the user in cache1
		cache1.Put(ctx, userID, user)

		// Try to get the user from cache2 with CacheOnly (should not exist)
		cachedUser, exists, err := cache2.Get(ctx, userID, CacheOnly())
		require.NoError(t, err)
		assert.False(t, exists)
		assert.Equal(t, TestUser{}, cachedUser)

		// Get the user from cache1 with CacheOnly (should exist)
		cachedUser, exists, err = cache1.Get(ctx, userID, CacheOnly())
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, user, cachedUser)

		// Notify remove from cache1
		err = cache1.NotifyRemove(userID)
		require.NoError(t, err)

		// Wait a bit for the notification to propagate
		time.Sleep(100 * time.Millisecond)

		// Try to get the user from both caches with CacheOnly (should not exist in either)
		cachedUser, exists, err = cache1.Get(ctx, userID, CacheOnly())
		require.NoError(t, err)
		assert.False(t, exists)
		assert.Equal(t, TestUser{}, cachedUser)

		cachedUser, exists, err = cache2.Get(ctx, userID, CacheOnly())
		require.NoError(t, err)
		assert.False(t, exists)
		assert.Equal(t, TestUser{}, cachedUser)
	})
}

func TestOutOfSyncBehavior(t *testing.T) {
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
		ChannelName:        "user_cache_updates",
		MaxSize:            100,
		Context:            ctx,
	}

	cache, err := NewCache[TestUser, uuid.UUID](db, config)
	require.NoError(t, err)
	defer cache.Shutdown()

	t.Run("OutOfSyncForcesRefresh", func(t *testing.T) {
		// Insert a user
		userID := uuid.New()
		_, err := db.ExecContext(ctx, `INSERT INTO "public"."users" (id, name) VALUES ($1, $2)`, userID, "John Doe")
		require.NoError(t, err)

		// Retrieve the user to cache it
		user, exists, err := cache.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "John Doe", user.Name)

		// Simulate a connection problem by using PostgreSQL's administrative disconnect
		_, err = db.ExecContext(ctx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid <> pg_backend_pid() AND usename = current_user`)
		require.NoError(t, err)

		// Wait a bit for the connection to be re-established
		time.Sleep(2 * time.Second)

		// Update the user directly in the database
		_, err = db.ExecContext(ctx, `UPDATE "public"."users" SET name = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`, "Jane Doe", userID)
		require.NoError(t, err)

		// Attempt to get the user again (this should force a refresh due to out-of-sync)
		updatedUser, exists, err := cache.Get(ctx, userID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Jane Doe", updatedUser.Name)
	})
}

func TestNotifyRemoveMultiple(t *testing.T) {
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
		ChannelName:        "user_cache_updates",
		MaxSize:            100,
		Context:            ctx,
	}

	cache, err := NewCache[TestUser, uuid.UUID](db, config)
	require.NoError(t, err)
	defer cache.Shutdown()

	t.Run("NotifyRemoveMultipleIDs", func(t *testing.T) {
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

		time.Sleep(100 * time.Millisecond)
		// Check that all users are removed from the cache
		for _, id := range userIDs {
			cache.mutex.RLock()
			_, exists := cache.cache[id]
			cache.mutex.RUnlock()
			assert.False(t, exists)
		}

		// Retrieve users again (should fetch from DB)
		for i, id := range userIDs {
			user, exists, err := cache.Get(ctx, id)
			require.NoError(t, err)
			assert.True(t, exists)
			assert.Equal(t, fmt.Sprintf("User %d", i+1), user.Name)
		}
	})
}

// New test for NotifyRemoveAndGetQuery with multiple IDs
func TestNotifyRemoveAndGetQueryMultiple(t *testing.T) {
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
		ChannelName:        "user_cache_updates",
		MaxSize:            100,
		Context:            ctx,
	}

	cache, err := NewCache[TestUser, uuid.UUID](db, config)
	require.NoError(t, err)
	defer cache.Shutdown()

	t.Run("NotifyRemoveAndGetQueryMultipleIDs", func(t *testing.T) {
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

		// Get the notify query for multiple users
		notifyQueryResult, err := cache.NotifyRemoveAndGetQuery(userIDs...)
		require.NoError(t, err)

		// Execute the notify query
		tx, err := db.BeginTx(ctx, nil)
		require.NoError(t, err)

		_, err = tx.ExecContext(ctx, notifyQueryResult.Query, notifyQueryResult.Params...)
		require.NoError(t, err)

		err = tx.Commit()
		require.NoError(t, err)

		time.Sleep(100 * time.Millisecond)
		// Check that all users are removed from the cache
		for _, id := range userIDs {
			cache.mutex.RLock()
			_, exists := cache.cache[id]
			cache.mutex.RUnlock()
			assert.False(t, exists)
		}

		// Retrieve users again (should fetch from DB)
		for i, id := range userIDs {
			user, exists, err := cache.Get(ctx, id)
			require.NoError(t, err)
			assert.True(t, exists)
			assert.Equal(t, fmt.Sprintf("User %d", i+1), user.Name)
		}
	})
}

func TestClearAllAndNotify(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	postgres, db, err := startPostgresContainer(ctx)
	require.NoError(t, err)
	defer postgres.Terminate(ctx)
	defer db.Close()

	schema := "test_schema"
	table := "users"
	err = setupTestTable(db, schema, table)
	require.NoError(t, err)

	config := CacheConfig{
		Schema:             schema,
		TableName:          table,
		IDFieldName:        "ID",
		UpdatedAtFieldName: "UpdatedAt",
		ChannelName:        "user_cache_updates",
		MaxSize:            100,
		Context:            ctx,
	}

	cache1, err := NewCache[TestUser, uuid.UUID](db, config)
	require.NoError(t, err)
	defer cache1.Shutdown()

	cache2, err := NewCache[TestUser, uuid.UUID](db, config)
	require.NoError(t, err)
	defer cache2.Shutdown()

	// Insert test users
	testUsers := make([]TestUser, 3)
	for i := 0; i < 3; i++ {
		user := TestUser{
			ID:   uuid.New(),
			Name: fmt.Sprintf("User %d", i+1),
		}
		testUsers[i] = user
		_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (id, name) VALUES ($1, $2)`, schema, table), user.ID, user.Name)
		require.NoError(t, err)
	}

	// Load users into both caches
	for _, user := range testUsers {
		_, exists, err := cache1.Get(ctx, user.ID)
		require.NoError(t, err)
		assert.True(t, exists)

		_, exists, err = cache2.Get(ctx, user.ID)
		require.NoError(t, err)
		assert.True(t, exists)
	}

	// Verify that users are in both caches
	for _, user := range testUsers {
		_, exists, _ := cache1.Get(ctx, user.ID, CacheOnly())
		assert.True(t, exists, "User should be in cache1")

		_, exists, _ = cache2.Get(ctx, user.ID, CacheOnly())
		assert.True(t, exists, "User should be in cache2")
	}

	// Call ClearAllAndNotify on cache1
	err = cache1.ClearAllAndNotify(ctx)
	require.NoError(t, err)

	// Wait a bit for the notification to propagate
	time.Sleep(100 * time.Millisecond)

	// Verify that both caches are cleared
	for _, user := range testUsers {
		_, exists, _ := cache1.Get(ctx, user.ID, CacheOnly())
		assert.False(t, exists, "User should not be in cache1 after ClearAllAndNotify")

		_, exists, _ = cache2.Get(ctx, user.ID, CacheOnly())
		assert.False(t, exists, "User should not be in cache2 after ClearAllAndNotify")
	}

	// Verify that we can still retrieve users from the database
	for _, user := range testUsers {
		retrievedUser, exists, err := cache1.Get(ctx, user.ID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, user.Name, retrievedUser.Name)

		retrievedUser, exists, err = cache2.Get(ctx, user.ID)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, user.Name, retrievedUser.Name)
	}
}
