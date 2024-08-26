package gopgcache

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/tzahifadida/pgln"
)

type CacheableRow[ID comparable] interface {
	GetID() ID
	GetUpdatedAt() time.Time
}

type Cache[C CacheableRow[ID], ID comparable] struct {
	db              *sqlx.DB
	pgln            *pgln.PGListenNotify
	cache           map[ID]*list.Element
	lru             *list.List
	mutex           sync.RWMutex
	schema          string
	tableName       string
	idColumn        string
	updatedAtColumn string
	outOfSync       bool
	channelName     string
	nodeID          uuid.UUID
	customPGLN      bool
	maxSize         int
	ctx             context.Context
	idColumnType    string
	logger          Logger
}

// Logger defines the interface for logging operations.
type Logger interface {
	Debug(msg string, args ...any)
	Error(msg string, args ...any)
}

type CacheConfig struct {
	Schema          string
	TableName       string
	IDColumn        string
	UpdatedAtColumn string
	ChannelName     string
	CustomPGLN      *pgln.PGListenNotify
	MaxSize         int
	Context         context.Context
	Logger          Logger
}

type cacheItem[C any, ID comparable] struct {
	value C
	id    ID
}

type CacheNotification[ID comparable] struct {
	NodeID uuid.UUID
	Action string
	IDs    []ID
}

func NewCache[C CacheableRow[ID], ID comparable](db *sqlx.DB, config CacheConfig) (*Cache[C, ID], error) {
	var pglnInstance *pgln.PGListenNotify
	var err error

	if config.CustomPGLN != nil {
		pglnInstance = config.CustomPGLN
	} else {
		builder := pgln.NewPGListenNotifyBuilder().
			SetContext(config.Context).
			SetReconnectInterval(1 * time.Second).
			SetDB(db.DB)

		pglnInstance, err = builder.Build()
		if err != nil {
			return nil, fmt.Errorf("failed to initialize pgln: %w", err)
		}

		err = pglnInstance.Start()
		if err != nil {
			return nil, fmt.Errorf("failed to start pgln: %w", err)
		}
	}

	nodeID, err := uuid.NewRandom()
	if err != nil {
		return nil, fmt.Errorf("failed to generate node ID: %w", err)
	}

	if config.Schema == "" {
		config.Schema = "public"
	}

	idColumnType, err := getColumnType(db, config.Schema, config.TableName, config.IDColumn)
	if err != nil {
		return nil, fmt.Errorf("failed to get ID column type: %w", err)
	}

	cache := &Cache[C, ID]{
		db:              db,
		pgln:            pglnInstance,
		cache:           make(map[ID]*list.Element),
		lru:             list.New(),
		schema:          config.Schema,
		tableName:       config.TableName,
		idColumn:        config.IDColumn,
		updatedAtColumn: config.UpdatedAtColumn,
		channelName:     config.ChannelName,
		nodeID:          nodeID,
		customPGLN:      config.CustomPGLN != nil,
		maxSize:         config.MaxSize,
		ctx:             config.Context,
		logger:          config.Logger,
		idColumnType:    idColumnType,
	}

	if cache.logger == nil {
		cache.logger = slog.Default()
	}
	err = pglnInstance.ListenAndWaitForListening(config.ChannelName, pgln.ListenOptions{
		NotificationCallback: cache.handleNotification,
		ErrorCallback: func(channel string, err error) {
			cache.logger.Error("PGLN error on channel %s: %v\n", channel, err)
			cache.mutex.Lock()
			cache.outOfSync = true
			cache.mutex.Unlock()
		},
		OutOfSyncBlockingCallback: cache.handleOutOfSync,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to listen on channel: %w", err)
	}

	return cache, nil
}

func getColumnType(db *sqlx.DB, schema, table, column string) (string, error) {
	query := `
		SELECT data_type 
		FROM information_schema.columns 
		WHERE table_schema = $1 AND table_name = $2 AND column_name = $3
	`
	var dataType string
	err := db.QueryRow(query, schema, table, column).Scan(&dataType)
	if err != nil {
		return "", fmt.Errorf("failed to get column type: %w", err)
	}
	return dataType, nil
}

func (c *Cache[C, ID]) Get(ctx context.Context, id ID) (C, bool, error) {
	c.mutex.RLock()
	outOfSync := c.outOfSync
	if !outOfSync {
		if el, exists := c.cache[id]; exists {
			c.mutex.RUnlock()
			c.mutex.Lock()
			c.lru.MoveToFront(el)
			c.mutex.Unlock()
			return el.Value.(cacheItem[C, ID]).value, true, nil
		}
	}
	c.mutex.RUnlock()

	row, err := c.loadFromDB(ctx, id)
	if err != nil {
		var zero C
		if err == sql.ErrNoRows {
			return zero, false, nil
		}
		return zero, false, err
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Always update the cache, even if we were previously out of sync
	if el, exists := c.cache[id]; exists {
		c.lru.MoveToFront(el)
		el.Value = cacheItem[C, ID]{value: row, id: id}
	} else {
		if c.lru.Len() >= c.maxSize {
			c.evict()
		}
		el := c.lru.PushFront(cacheItem[C, ID]{value: row, id: id})
		c.cache[id] = el
	}

	return row, true, nil
}

func (c *Cache[C, ID]) loadFromDB(ctx context.Context, id ID) (C, error) {
	query := fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE "%s" = $1`, c.schema, c.tableName, c.idColumn)
	var item C
	err := c.db.GetContext(ctx, &item, query, id)
	if err != nil {
		return item, fmt.Errorf("failed to load item from database: %w", err)
	}
	return item, nil
}

func (c *Cache[C, ID]) evict() {
	el := c.lru.Back()
	if el != nil {
		c.lru.Remove(el)
		delete(c.cache, el.Value.(cacheItem[C, ID]).id)
	}
}

func (c *Cache[C, ID]) NotifyRemove(ids ...ID) error {
	c.mutex.Lock()
	for _, id := range ids {
		if el, exists := c.cache[id]; exists {
			c.lru.Remove(el)
			delete(c.cache, id)
		}
	}
	c.mutex.Unlock()

	notification := CacheNotification[ID]{
		NodeID: c.nodeID,
		Action: "remove",
		IDs:    ids,
	}

	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal notification: %w", err)
	}

	notifyQuery := c.pgln.NotifyQuery(c.channelName, string(notificationJSON))
	_, err = c.db.ExecContext(c.ctx, notifyQuery.Query, notifyQuery.Params...)
	if err != nil {
		return fmt.Errorf("failed to send notification: %w", err)
	}

	return nil
}

func (c *Cache[C, ID]) handleNotification(channel string, payload string) {
	var notification CacheNotification[ID]

	err := json.Unmarshal([]byte(payload), &notification)
	if err != nil {
		c.logger.Error("Error unmarshalling notification: %v\n", err)
		return
	}

	if notification.NodeID == c.nodeID {
		return
	}

	if notification.Action == "remove" {
		c.mutex.Lock()
		for _, id := range notification.IDs {
			if el, exists := c.cache[id]; exists {
				c.lru.Remove(el)
				delete(c.cache, id)
			}
		}
		c.mutex.Unlock()
	}
}

func (c *Cache[C, ID]) handleOutOfSync(channel string) error {
	c.logger.Debug("Out of sync detected on channel %s, refreshing cache", channel)
	return c.refreshCache(c.ctx)
}

func (c *Cache[C, ID]) refreshCache(ctx context.Context) error {
	if c.updatedAtColumn == "" {
		c.ClearCache()
		return nil
	}

	var cachedIDs []ID
	var mostRecentUpdate time.Time

	c.mutex.RLock()
	for id, el := range c.cache {
		cachedIDs = append(cachedIDs, id)
		itemUpdatedAt := el.Value.(cacheItem[C, ID]).value.GetUpdatedAt()
		if itemUpdatedAt.After(mostRecentUpdate) {
			mostRecentUpdate = itemUpdatedAt
		}
	}
	c.mutex.RUnlock()

	if len(cachedIDs) == 0 {
		return nil
	}

	threshold := mostRecentUpdate.Add(-2 * time.Second)

	placeholders := c.generatePlaceholders(len(cachedIDs), c.idColumnType)

	query := fmt.Sprintf(`
		WITH existing(id) AS (
			VALUES %s
		)
		SELECT existing.id FROM existing
		LEFT JOIN "%s"."%s" t ON existing.id = t."%s"::%s
		WHERE t."%s" IS NULL
		UNION
		SELECT t."%s" FROM "%s"."%s" t
		JOIN existing ON t."%s" = existing.id::%s
		WHERE t."%s" > $%d
	`, placeholders,
		c.schema, c.tableName, c.idColumn, c.idColumnType,
		c.idColumn,
		c.idColumn, c.schema, c.tableName,
		c.idColumn, c.idColumnType,
		c.updatedAtColumn, len(cachedIDs)+1)

	args := make([]interface{}, len(cachedIDs)+1)
	for i, id := range cachedIDs {
		args[i] = id
	}
	args[len(cachedIDs)] = threshold

	rows, err := c.db.QueryContext(ctx, query, args...)
	if err != nil {
		return fmt.Errorf("failed to query rows to refresh: %w", err)
	}
	defer rows.Close()

	var toRemoveIDs []ID
	for rows.Next() {
		var id ID
		if err := rows.Scan(&id); err != nil {
			return fmt.Errorf("failed to scan row id: %w", err)
		}
		toRemoveIDs = append(toRemoveIDs, id)
	}

	c.mutex.Lock()
	for _, id := range toRemoveIDs {
		if el, exists := c.cache[id]; exists {
			c.lru.Remove(el)
			delete(c.cache, id)
		}
	}
	c.mutex.Unlock()

	return nil
}

func (c *Cache[C, ID]) generatePlaceholders(n int, columnType string) string {
	placeholders := make([]string, n)
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("($%d::%s)", i+1, columnType)
	}
	return strings.Join(placeholders, ", ")
}

func (c *Cache[C, ID]) ClearCache() {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	c.cache = make(map[ID]*list.Element)
	c.lru = list.New()
}

func (c *Cache[C, ID]) Shutdown() {
	if !c.customPGLN {
		c.pgln.Shutdown()
	}
}
