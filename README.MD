# go-pg-cache

`go-pg-cache` is a distributed caching library for Go that uses PostgreSQL as a backend. It leverages PostgreSQL's LISTEN/NOTIFY mechanism to handle cache invalidation across multiple nodes, ensuring consistency in distributed environments.

⭐️ **Star This Project** ⭐️

If you find this project helpful, please give it a star on GitHub! Your support is greatly appreciated.

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
- [Configuration](#configuration)
- [Examples](#examples)
- [Performance Considerations](#performance-considerations)
- [Contributing](#contributing)
- [License](#license)

## Features

- **Distributed Caching**: Uses PostgreSQL LISTEN/NOTIFY for real-time cache invalidation across nodes.
- **Generic Implementation**: Supports caching of any struct that implements the `CacheableRow` interface.
- **LRU Eviction**: Implements an LRU (Least Recently Used) eviction policy for the in-memory cache.
- **Configurable**: Various settings can be customized via a configuration structure.
- **Efficient Querying**: Optimized database queries for refreshing the cache.
- **Logging**: Integrated logging for easier debugging and monitoring.

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
)

// Define your struct
type User struct {
    ID        int       `db:"id"`
    Name      string    `db:"name"`
    UpdatedAt time.Time `db:"updated_at"`
}

// Implement CacheableRow interface
func (u User) GetID() int {
    return u.ID
}

func (u User) GetUpdatedAt() time.Time {
    return u.UpdatedAt
}

// Initialize the cache
db, _ := sqlx.Connect("pgx", "your_connection_string")
config := gopgcache.CacheConfig{
    Schema:          "public",
    TableName:       "users",
    IDColumn:        "id",
    UpdatedAtColumn: "updated_at",
    ChannelName:     "cache_invalidation",
    MaxSize:         1000,
    Context:         context.Background(),
}

cache, err := gopgcache.NewCache[User, int](db, config)
if err != nil {
    log.Fatalf("Failed to create cache: %v", err)
}

// Use the cache
user, exists, err := cache.Get(context.Background(), 1)
if err != nil {
    log.Printf("Error getting user: %v", err)
} else if exists {
    fmt.Printf("User: %v\n", user)
}

// Invalidate cache entry
err = cache.NotifyRemove(1)
if err != nil {
    log.Printf("Error notifying removal: %v", err)
}
```

## Configuration

The `CacheConfig` struct allows you to customize the behavior of the cache:

- `Schema`: The database schema name.
- `TableName`: The name of the table being cached.
- `IDColumn`: The name of the ID column in the table.
- `UpdatedAtColumn`: The name of the column indicating when a row was last updated.
- `ChannelName`: The PostgreSQL LISTEN/NOTIFY channel name for cache invalidation.
- `CustomPGLN`: Optional custom PGLN instance.
- `MaxSize`: Maximum number of items to keep in the in-memory cache.
- `Context`: Context for database operations.
- `Logger`: Custom logger implementing the `Logger` interface.

## Examples

### Creating a Cache

```go
cache, err := gopgcache.NewCache[User, int](db, config)
if err != nil {
    log.Fatalf("Failed to create cache: %v", err)
}
defer cache.Shutdown()
```

### Retrieving an Item

```go
user, exists, err := cache.Get(context.Background(), userID)
if err != nil {
    log.Printf("Error retrieving user: %v", err)
} else if exists {
    fmt.Printf("User: %+v\n", user)
} else {
    fmt.Println("User not found")
}
```

### Invalidating a Cache Entry

```go
err := cache.NotifyRemove(userID)
if err != nil {
    log.Printf("Error invalidating cache entry: %v", err)
}
```

### Handling Cache Misses

```go
user, exists, err := cache.Get(context.Background(), userID)
if err != nil {
    log.Printf("Error retrieving user: %v", err)
} else if !exists {
    // Handle cache miss, e.g., fetch from another source
    user = fetchUserFromAnotherSource(userID)
    // You might want to add it to the cache here
}
```

## Performance Considerations

- **Cache Size**: Adjust the `MaxSize` in `CacheConfig` based on your application's memory constraints and the size of your dataset.
- **Database Queries**: The library uses optimized queries to refresh the cache, but be mindful of the load on your PostgreSQL server.
- **Invalidation Strategy**: Use `NotifyRemove` judiciously to keep the cache consistent without overwhelming the system with notifications.
- **Monitoring**: Keep an eye on cache hit rates and database query times to fine-tune your configuration.

## Contributing

Contributions to `go-pg-cache` are welcome! Here are some ways you can contribute:

1. Report bugs or request features by opening an issue.
2. Improve documentation.
3. Submit pull requests with bug fixes or new features.

Please ensure that your code adheres to the existing style and that all tests pass before submitting a pull request.

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

Thank you for using `go-pg-cache`! If you have any questions, suggestions, or encounter any issues, please don't hesitate to open an issue on the GitHub repository. Your feedback and contributions are greatly appreciated and help make this library better for everyone.