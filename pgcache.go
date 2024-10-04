package gopgcache

import (
	"container/list"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/jmoiron/sqlx"
	"github.com/tzahifadida/pgln"
)

type Cache[C any, ID comparable] struct {
	db                  *sqlx.DB
	pgln                *pgln.PGListenNotify
	cache               map[ID]*list.Element
	lru                 *list.List
	mutex               sync.RWMutex
	schema              string
	tableName           string
	outOfSync           bool
	channelName         string
	nodeID              uuid.UUID
	customPGLN          bool
	maxSize             int
	ctx                 context.Context
	logger              Logger
	idField             reflect.StructField
	updatedAtField      reflect.StructField
	idFieldName         string
	updatedAtFieldName  string
	idColumnName        string
	updatedAtColumnName string
	idColumnType        string
}

type Logger interface {
	Debug(msg string, args ...any)
	Error(msg string, args ...any)
}

type CacheConfig struct {
	Schema             string
	TableName          string
	ChannelName        string
	CustomPGLN         *pgln.PGListenNotify
	MaxSize            int
	Context            context.Context
	Logger             Logger
	IDFieldName        string
	UpdatedAtFieldName string
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

func NewCache[C any, ID comparable](db *sqlx.DB, config CacheConfig) (*Cache[C, ID], error) {
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

	var zero C
	t := reflect.TypeOf(zero)
	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("C must be a struct type")
	}

	idField, updatedAtField, idColumnName, updateAtColumnName, err :=
		findFields(t, config.IDFieldName, config.UpdatedAtFieldName)
	if err != nil {
		return nil, fmt.Errorf("failed to find required fields: %w", err)
	}

	var timePointer *time.Time
	if updatedAtField.Type != reflect.TypeOf(time.Time{}) && updatedAtField.Type != reflect.TypeOf(timePointer) {
		return nil, fmt.Errorf("field %s is not of type time.Time", config.UpdatedAtFieldName)
	}

	idColumnType, err := getColumnType(db, config.Schema, config.TableName, idColumnName)
	if err != nil {
		return nil, fmt.Errorf("failed to get ID column type: %w", err)
	}

	cache := &Cache[C, ID]{
		db:                  db,
		pgln:                pglnInstance,
		cache:               make(map[ID]*list.Element),
		lru:                 list.New(),
		schema:              config.Schema,
		tableName:           config.TableName,
		channelName:         config.ChannelName,
		nodeID:              nodeID,
		customPGLN:          config.CustomPGLN != nil,
		maxSize:             config.MaxSize,
		ctx:                 config.Context,
		logger:              config.Logger,
		idField:             idField,
		updatedAtField:      updatedAtField,
		idFieldName:         config.IDFieldName,
		updatedAtFieldName:  config.UpdatedAtFieldName,
		idColumnName:        idColumnName,
		updatedAtColumnName: updateAtColumnName,
		idColumnType:        idColumnType,
	}

	if cache.logger == nil {
		cache.logger = slog.Default()
	}

	err = pglnInstance.ListenAndWaitForListening(config.ChannelName, pgln.ListenOptions{
		NotificationCallback: cache.handleNotification,
		ErrorCallback: func(channel string, err error) {
			cache.logger.Error("PGLN error on channel %s: %v", channel, err)
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

func findFields(t reflect.Type, idFieldName, updatedAtFieldName string) (reflect.StructField, reflect.StructField, string, string, error) {
	var idField, updatedAtField reflect.StructField
	var idTag, updatedAtTag string
	var ok bool

	if idField, ok = t.FieldByName(idFieldName); !ok {
		return reflect.StructField{}, reflect.StructField{}, "", "", fmt.Errorf("struct does not have a field named %s", idFieldName)
	}

	if idTag, ok = idField.Tag.Lookup("db"); !ok {
		return reflect.StructField{}, reflect.StructField{}, "", "", fmt.Errorf("field %s does not have a 'db' tag", idFieldName)
	}

	if updatedAtFieldName != "" {
		if updatedAtField, ok = t.FieldByName(updatedAtFieldName); !ok {
			return reflect.StructField{}, reflect.StructField{}, "", "", fmt.Errorf("struct does not have a field named %s", updatedAtFieldName)
		}

		if updatedAtTag, ok = updatedAtField.Tag.Lookup("db"); !ok {
			return reflect.StructField{}, reflect.StructField{}, "", "", fmt.Errorf("field %s does not have a 'db' tag", updatedAtFieldName)
		}
	}

	return idField, updatedAtField, idTag, updatedAtTag, nil
}

type GetOption func(*getOptions)

type getOptions struct {
	cacheOnly bool
}

func CacheOnly() GetOption {
	return func(o *getOptions) {
		o.cacheOnly = true
	}
}

func (c *Cache[C, ID]) Get(ctx context.Context, id ID, opts ...GetOption) (C, bool, error) {
	options := getOptions{}
	for _, opt := range opts {
		opt(&options)
	}

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

	if options.cacheOnly {
		var zero C
		return zero, false, nil
	}

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

	rowValue := reflect.ValueOf(row)
	idValue := rowValue.FieldByIndex(c.idField.Index).Interface().(ID)

	if el, exists := c.cache[id]; exists {
		c.lru.MoveToFront(el)
		el.Value = cacheItem[C, ID]{value: row, id: idValue}
	} else {
		if c.lru.Len() >= c.maxSize {
			c.evict()
		}
		el := c.lru.PushFront(cacheItem[C, ID]{value: row, id: idValue})
		c.cache[idValue] = el
	}

	return row, true, nil
}

func (c *Cache[C, ID]) loadFromDB(ctx context.Context, id ID) (C, error) {
	query := fmt.Sprintf(`SELECT * FROM "%s"."%s" WHERE "%s" = $1`, c.schema, c.tableName, c.idColumnName)

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
	notifyQueryResult, err := c.NotifyRemoveAndGetQuery(ids...)
	if err != nil {
		return fmt.Errorf("failed to get notify query: %w", err)
	}

	_, err = c.db.ExecContext(c.ctx, notifyQueryResult.Query, notifyQueryResult.Params...)
	if err != nil {
		return fmt.Errorf("failed to execute notify query: %w", err)
	}

	return nil
}

type NotifyQueryResult struct {
	Query  string
	Params []interface{}
}

func (c *Cache[C, ID]) NotifyRemoveAndGetQuery(ids ...ID) (NotifyQueryResult, error) {
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
		return NotifyQueryResult{}, fmt.Errorf("failed to marshal notification: %w", err)
	}

	pglnResult := c.pgln.NotifyQuery(c.channelName, string(notificationJSON))

	return NotifyQueryResult{
		Query:  pglnResult.Query,
		Params: pglnResult.Params,
	}, nil
}

func (c *Cache[C, ID]) Put(ctx context.Context, id ID, row C) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

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
}

func (c *Cache[C, ID]) handleNotification(channel string, payload string) {
	var notification CacheNotification[ID]

	err := json.Unmarshal([]byte(payload), &notification)
	if err != nil {
		c.logger.Error("Error unmarshalling notification: %v", err)
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
	} else if notification.Action == "clear_all" {
		c.ClearCache()
		c.logger.Debug("Cleared entire cache due to clear_all notification")
	} else {
		c.logger.Error("Unsupported notification action: %v", notification.Action)
	}
}

func (c *Cache[C, ID]) handleOutOfSync(channel string) error {
	c.logger.Debug("Out of sync detected on channel %s, refreshing cache", channel)
	return c.refreshCache(c.ctx)
}

func (c *Cache[C, ID]) refreshCache(ctx context.Context) error {
	if c.updatedAtField.Index == nil {
		c.ClearCache()
		return nil
	}

	var cachedIDs []ID
	var mostRecentUpdate time.Time

	c.mutex.RLock()
	for id, el := range c.cache {
		cachedIDs = append(cachedIDs, id)
		itemValue := reflect.ValueOf(el.Value.(cacheItem[C, ID]).value)
		updatedAt := itemValue.FieldByIndex(c.updatedAtField.Index).Interface().(time.Time)
		if updatedAt.After(mostRecentUpdate) {
			mostRecentUpdate = updatedAt
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
		LEFT JOIN "%s"."%s" t ON existing.id = t."%s"
		WHERE t."%s" IS NULL
		UNION
		SELECT t."%s" FROM "%s"."%s" t
		JOIN existing ON t."%s" = existing.id
        WHERE t."%s" > $%d
	`, placeholders,
		c.schema, c.tableName, c.idColumnName,
		c.idColumnName,
		c.idColumnName, c.schema, c.tableName,
		c.idColumnName,
		c.updatedAtColumnName, len(cachedIDs)+1)

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

// ClearAllAndNotify clears the entire local cache and sends a notification to all nodes to clear their caches
func (c *Cache[C, ID]) ClearAllAndNotify(ctx context.Context) error {
	// Clear the local cache
	c.ClearCache()

	// Prepare the notification
	notification := CacheNotification[ID]{
		NodeID: c.nodeID,
		Action: "clear_all",
		IDs:    nil, // No specific IDs, as we're clearing everything
	}

	notificationJSON, err := json.Marshal(notification)
	if err != nil {
		return fmt.Errorf("failed to marshal clear all notification: %w", err)
	}
	pglnResult := c.pgln.NotifyQuery(c.channelName, string(notificationJSON))
	// Send the notification
	_, err = c.db.ExecContext(ctx, pglnResult.Query, pglnResult.Params...)
	if err != nil {
		return fmt.Errorf("failed to send clear all notification: %w", err)
	}

	return nil
}
