## Cache

SquashQL provides an in-memory query cache to not re-execute queries already executed. It has two main benefits:

- It reduces the need to frequently retrieve data from the database, thereby lowering the overall load on the
  database server and potentially reducing the resources required for database operations.
- It speeds up queries by holding frequently requested data.

By default, an implementation with the following characteristic is provided
- Cache is user based if a bean of type `Supplier<SquashQLUser>` is provided. Otherwise, it is shared with all users
- 32 results are stored per user by default. This can be configured using the system property: `-Dio.squashql.cache.size=64`
- Results older than 5 minutes are discarded. It can be changed using the system property: `-Dio.squashql.cache.duration=10` (in minutes) 

### Cache invalidation

The results older than 5 minutes are considered as invalid, but it is possible to invalidate the cache when executing a 
query, see  [QUERY.md#query-cache](QUERY.md#query-cache). 

If a different strategy needs to be implemented, the cache is accessible from the `QueryController` object:

```java
@Component
public class MyComponent {

  public MyComponent(QueryController queryController) {
    QueryCache queryCache = queryController.queryExecutor.queryCache;
    // TODO 
  }
}
```

The method `void clear()` or `void clear(SquashQLUser user)` are to be called to invalidate the cache.

### Statistics

The current implementation provides basic statistics to monitor cache usage. 

```
cache.getHistogram()
```

returns a histogram of number of tables per table size ranges. Size of table = number of rows * number of columns. 
For instance:

```
[1-100:2],[101-1000:1],[1001-10000:3],[10001-50000:1],[50001-100000:0],[100001-200000:0],[200001-500000:0],[500001-1000000:0],[1000000<:1]
```

2 table have a size between 1 and 100, 1 table has a size between 101 and 1000, 3 tables have a size between 1001 and 10000...

### More details

The code can be found in the classes `GlobalCache` and `CaffeineQueryCache`. See usage in `QueryExecutor`.
