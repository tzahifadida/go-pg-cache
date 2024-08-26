# go-pg-cache

`go-pg-cache` is a specialized caching library for Go that uses PostgreSQL as a backend. It's designed to cache a
limited subset of a table that is frequently accessed, providing significant performance improvements for repeated
queries on the same data.

The library leverages PostgreSQL's LISTEN/NOTIFY mechanism to handle cache invalidation across multiple nodes, ensuring
consistency in distributed environments.

⭐️ **Star This Project** ⭐️

If you find this project helpful, please give it a star on GitHub! Your support is greatly appreciated.

## Table of Contents

- [Purpose and Ideal Use Cases](#purpose-and-ideal-use-cases)
- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Examples](#examples)
- [Performance Considerations](#performance-considerations)
- [Contributing](#contributing)
- [License](#license)

## Purpose and Ideal Use Cases

The primary purpose of `go-pg-cache` is to provide a caching layer for frequently accessed, relatively static data in
PostgreSQL tables. It's particularly useful in scenarios where:

1. A subset of the table is accessed much more frequently than the rest.
2. The data doesn't change very often.
3. The same queries are repeated multiple times.

Ideal use cases include:

- **User Profiles**: Caching frequently accessed user profile data.
- **Account/Tenant Information**: For multi-tenant applications where tenant data is often queried.
- **Configuration Settings**: Storing application or user settings that are read often but updated infrequently.
- **Product Catalogs**: Caching frequently viewed products in an e-commerce application.
- **Permission Sets**: Storing user roles and permissions that are checked frequently.

It's important to note that `go-pg-cache` is not designed for:

- Tables with rapidly changing data.
- Tables where access patterns are uniformly distributed (i.e., no subset of rows is accessed more frequently than
  others).
- Scenarios where data consistency is critical and even momentary inconsistencies are unacceptable.

By using `go-pg-cache` for appropriate use cases, you can significantly reduce database load and improve application
performance.

## Features

- **Selective Caching**: Designed to cache a limited, frequently accessed subset of your PostgreSQL table.
- **Distributed Caching**: Uses PostgreSQL LISTEN/NOTIFY for real-time cache invalidation across nodes, ensuring
  consistency in distributed environments.
- **Generic Implementation**: Supports caching of any struct that implements the `CacheableRow` interface, allowing
  flexibility in what data is cached.
- **UUID Support**: Uses UUIDs as the primary key type for enhanced uniqueness and compatibility with modern application
  designs.
- **Bulk Invalidation**: Supports invalidating multiple cache entries at once, useful for related data updates.
- **LRU Eviction**: Implements an LRU (Least Recently Used) eviction policy for the in-memory cache, ensuring the most
  relevant data is kept in cache.
- **Configurable**: Various settings can be customized via a configuration structure, allowing you to fine-tune the
  cache for your specific use case.
- **Efficient Querying**: Optimized database queries for refreshing the cache, minimizing the performance impact of
  cache misses.
- **Automatic Synchronization**: Handles out-of-sync scenarios gracefully, ensuring cache consistency even after network
  issues or application restarts.
- **Logging**: Integrated logging for easier debugging and monitoring of cache operations.

## Installation

To install the package, run:

```sh
go get github.com/tzahifadida/go-pg-cache
```

## Usage

Here's a basic example of how to use `go-pg-cache`:

```go
import (
"github.com/tzahifadida/go-pg-cache"
"github.com/jmoiron/sqlx"
"github.com/google/uuid"
"context"
"log"
"time"
"fmt"
)

// Define your struct
type User struct {
    ID        uuid.UUID `db:"id"`
    Name      string    `db:"name"`
    UpdatedAt time.Time `db:"updated_at"`
}

// Initialize the cache
db, _ := sqlx.Connect("pgx", "your_connection_string")
config := gopgcache.CacheConfig{
    Schema:          "public",
    TableName:       "users",
    IDFieldName:     "ID",
    UpdatedAtFieldName: "UpdatedAt",
    ChannelName:     "cache_invalidation",
    MaxSize:         1000,
    Context:         context.Background(),
}

cache, err := gopgcache.NewCache[User, uuid.UUID](db, config)
if err != nil {
    log.Fatalf("Failed to create cache: %v", err)
}

// Use the cache
userID := uuid.New()
user, exists, err := cache.Get(context.Background(), userID)
if err != nil {
    log.Printf("Error getting user: %v", err)
} else if exists {
    fmt.Printf("User: %v\n", user)
}

// Invalidate cache entries
err = cache.NotifyRemove(userID, uuid.New(), uuid.New())
if err != nil {
    log.Printf("Error notifying removal: %v", err)
}

```

## Configuration

The `CacheConfig` struct allows you to customize the behavior of the cache:

- `Schema`: The database schema name.
- `TableName`: The name of the table being cached.
- `IDFieldName`: The name of the ID field in the struct.
- `UpdatedAtFieldName`: The name of the field in the struct indicating when a row was last updated.
- `ChannelName`: The PostgreSQL LISTEN/NOTIFY channel name for cache invalidation.
- `CustomPGLN`: Optional custom PGLN instance.
- `MaxSize`: Maximum number of items to keep in the in-memory cache.
- `Context`: Context for database operations.
- `Logger`: Custom logger implementing the `Logger` interface.

## Examples

### Creating a Cache

```go
cache, err := gopgcache.NewCache[User, uuid.UUID](db, config)
if err != nil {
    log.Fatalf("Failed to create cache: %v", err)
}
defer cache.Shutdown()
```

### Retrieving an Item

```go
userID := uuid.New()
user, exists, err := cache.Get(context.Background(), userID)
if err != nil {
    log.Printf("Error retrieving user: %v", err)
} else if exists {
    fmt.Printf("User: %+v\n", user)
} else {
    fmt.Println("User not found")
}
```

### Invalidating Multiple Cache Entries

```go
err := cache.NotifyRemove(userID1, userID2, userID3)
if err != nil {
    log.Printf("Error invalidating cache entries: %v", err)
}
```

## Performance Considerations

- **Cache Size**: Adjust the `MaxSize` in `CacheConfig` based on your application's memory constraints and the size of
  your frequently accessed dataset.
- **Database Queries**: The library uses optimized queries to refresh the cache, but be mindful of the load on your
  PostgreSQL server, especially for large tables.
- **Invalidation Strategy**: Use `NotifyRemove` judiciously to keep the cache consistent without overwhelming the system
  with notifications.
- **Bulk Invalidations**: When possible, use `NotifyRemove` with multiple IDs to efficiently invalidate several cache
  entries at once.
- **Monitoring**: Keep an eye on cache hit rates and database query times to fine-tune your configuration and ensure the
  cache is providing the expected performance benefits.
- **Data Access Patterns**: Regularly analyze your data access patterns to ensure you're caching the most frequently
  accessed subset of your data.

## Contributing

Contributions to `go-pg-cache` are welcome! Here are some ways you can contribute:

1. Report bugs or request features by opening an issue.
2. Improve documentation.
3. Submit pull requests with bug fixes or new features.

Please ensure that your code adheres to the existing style and that all tests pass before submitting a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Thank you for using `go-pg-cache`! If you have any questions, suggestions, or encounter any issues, please don't
hesitate to open an issue on the GitHub repository. Your feedback and contributions are greatly appreciated and help
make this library better for everyone.