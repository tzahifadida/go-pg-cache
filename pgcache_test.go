package gopgcache

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

type TestUser struct {
	ID        int       `db:"id"`
	Name      string    `db:"name"`
	UpdatedAt time.Time `db:"updated_at"`
}

func (u TestUser) GetID() int {
	return u.ID
}

func (u TestUser) GetUpdatedAt() time.Time {
	return u.UpdatedAt
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
		CREATE TABLE "%s"."%s" (
			id SERIAL PRIMARY KEY,
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
		Schema:          schema,
		TableName:       table,
		IDColumn:        "id",
		UpdatedAtColumn: "updated_at",
		ChannelName:     "user_cache_updates",
		MaxSize:         100,
		Context:         ctx,
	}

	cache, err := NewCache[TestUser, int](db, config)
	require.NoError(t, err)
	defer cache.Shutdown()

	t.Run("InsertAndRetrieve", func(t *testing.T) {
		// Insert a user
		_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (name) VALUES ($1)`, schema, table), "John Doe")
		require.NoError(t, err)

		// Retrieve the user through the cache
		user, exists, err := cache.Get(ctx, 1)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "John Doe", user.Name)

		// Retrieve again (should be from cache)
		cachedUser, exists, err := cache.Get(ctx, 1)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, user, cachedUser)
	})

	t.Run("UpdateAndRetrieve", func(t *testing.T) {
		// Update the user
		_, err := db.ExecContext(ctx, fmt.Sprintf(`UPDATE "%s"."%s" SET name = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`, schema, table), "Jane Doe", 1)
		require.NoError(t, err)

		// Notify cache to remove the updated user
		err = cache.NotifyRemove(1)
		require.NoError(t, err)

		// Retrieve the updated user
		updatedUser, exists, err := cache.Get(ctx, 1)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Jane Doe", updatedUser.Name)
	})

	t.Run("DeleteAndRetrieve", func(t *testing.T) {
		// Delete the user
		_, err := db.ExecContext(ctx, fmt.Sprintf(`DELETE FROM "%s"."%s" WHERE id = $1`, schema, table), 1)
		require.NoError(t, err)

		// Notify cache to remove the deleted user
		err = cache.NotifyRemove(1)
		require.NoError(t, err)

		// Try to retrieve the deleted user
		_, exists, err := cache.Get(ctx, 1)
		assert.ErrorIs(t, err, sql.ErrNoRows) // We expect no error, just non-existence
		assert.False(t, exists)
	})

	t.Run("CacheEviction", func(t *testing.T) {
		// Create a new cache with a small size
		smallCache, err := NewCache[TestUser, int](db, CacheConfig{
			Schema:          schema,
			TableName:       table,
			IDColumn:        "id",
			UpdatedAtColumn: "updated_at",
			ChannelName:     "user_cache_updates",
			MaxSize:         2,
			Context:         ctx,
		})
		require.NoError(t, err)
		defer smallCache.Shutdown()

		// Insert three users
		for i := 2; i <= 4; i++ {
			_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (name) VALUES ($1)`, schema, table), fmt.Sprintf("User %d", i))
			require.NoError(t, err)
		}

		// Retrieve all three users
		for i := 2; i <= 4; i++ {
			_, exists, err := smallCache.Get(ctx, i)
			require.NoError(t, err)
			assert.True(t, exists)
		}

		// Check that only the last two users are in the cache
		smallCache.mutex.RLock()
		assert.Equal(t, 2, len(smallCache.cache))
		_, exists := smallCache.cache[2]
		assert.False(t, exists)
		_, exists = smallCache.cache[3]
		assert.True(t, exists)
		_, exists = smallCache.cache[4]
		assert.True(t, exists)
		smallCache.mutex.RUnlock()
	})

	t.Run("RefreshCache", func(t *testing.T) {
		// Insert a new user
		_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (name) VALUES ($1)`, schema, table), "Refresh Test")
		require.NoError(t, err)

		// Retrieve the user to cache it
		user, exists, err := cache.Get(ctx, 5)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Refresh Test", user.Name)

		// Update the user directly in the database
		_, err = db.ExecContext(ctx, fmt.Sprintf(`UPDATE "%s"."%s" SET name = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`, schema, table), "Refreshed User", 5)
		require.NoError(t, err)

		// Manually trigger a cache refresh
		err = cache.refreshCache(ctx)
		require.NoError(t, err)

		// Retrieve the user again
		refreshedUser, exists, err := cache.Get(ctx, 5)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Refreshed User", refreshedUser.Name)
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
		Schema:          schema,
		TableName:       table,
		IDColumn:        "id",
		UpdatedAtColumn: "updated_at",
		ChannelName:     "user_cache_updates",
		MaxSize:         100,
		Context:         ctx,
	}

	cache1, err := NewCache[TestUser, int](db, config)
	require.NoError(t, err)
	defer cache1.Shutdown()

	cache2, err := NewCache[TestUser, int](db, config)
	require.NoError(t, err)
	defer cache2.Shutdown()

	t.Run("NotificationBetweenCaches", func(t *testing.T) {
		// Insert a user
		_, err := db.ExecContext(ctx, fmt.Sprintf(`INSERT INTO "%s"."%s" (name) VALUES ($1)`, schema, table), "Multi Cache Test")
		require.NoError(t, err)

		// Retrieve the user in both caches
		user1, exists, err := cache1.Get(ctx, 1)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Multi Cache Test", user1.Name)

		user2, exists, err := cache2.Get(ctx, 1)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Multi Cache Test", user2.Name)

		// Update the user
		_, err = db.ExecContext(ctx, fmt.Sprintf(`UPDATE "%s"."%s" SET name = $1, updated_at = CURRENT_TIMESTAMP WHERE id = $2`, schema, table), "Updated Multi Cache", 1)
		require.NoError(t, err)

		// Notify removal from cache1
		err = cache1.NotifyRemove(1)
		require.NoError(t, err)

		// Wait a bit for the notification to propagate
		time.Sleep(100 * time.Millisecond)

		// Check that the user is removed from both caches
		cache1.mutex.RLock()
		_, exists = cache1.cache[1]
		cache1.mutex.RUnlock()
		assert.False(t, exists)

		cache2.mutex.RLock()
		_, exists = cache2.cache[1]
		cache2.mutex.RUnlock()
		assert.False(t, exists)

		// Retrieve the updated user in both caches
		updatedUser1, exists, err := cache1.Get(ctx, 1)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Updated Multi Cache", updatedUser1.Name)

		updatedUser2, exists, err := cache2.Get(ctx, 1)
		require.NoError(t, err)
		assert.True(t, exists)
		assert.Equal(t, "Updated Multi Cache", updatedUser2.Name)
	})
}
