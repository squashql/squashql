package me.paulbares.query;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import me.paulbares.query.context.ContextValue;
import me.paulbares.query.context.Repository;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.ScenarioGroupingQueryDto;
import me.paulbares.query.dto.TableDto;

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

public class ScenarioGroupingCache {

  private static final Logger logger = Logger.getLogger(ScenarioGroupingCache.class.getName());

  public final QueryEngine queryEngine;

  protected final LoadingCache<CacheKey, CacheValue> cache;

  public ScenarioGroupingCache(QueryEngine queryEngine) {
    this.queryEngine = queryEngine;
    this.cache = CacheBuilder.newBuilder()
            .maximumSize(32)
            .expireAfterWrite(8, TimeUnit.MINUTES)
            .recordStats()
            .build(
                    new CacheLoader<>() {
                      public CacheValue load(CacheKey key) {
                        QueryDto prefetchQuery = new QueryDto()
                                .wildcardCoordinate(SCENARIO_FIELD_NAME)
                                .table(key.table)
                                .context(Repository.KEY, key.repository());
                        prefetchQuery.measures.addAll(key.measures);
                        return new CacheValue(queryEngine.execute(prefetchQuery));
                      }
                    });
  }

  public Table get(ScenarioGroupingQueryDto query) {
    try {
      CacheValue cacheValue = this.cache.get(createKey(query));
      return cacheValue.table;
    } catch (ExecutionException e) {
      logger.log(Level.SEVERE, e, () -> "error while executing a query");
      throw new RuntimeException(e);
    }
  }

  private CacheKey createKey(ScenarioGroupingQueryDto q) {
    return new CacheKey(
            q.table,
            q.groups.values().stream().flatMap(v -> v.stream()).collect(Collectors.toSet()),
            q.comparisons.stream().map(c -> c.measure).toList(),
            q.context.get(Repository.KEY));
  }

  record CacheKey(TableDto table, Set<String> scenarios, List<Measure> measures, ContextValue repository) {
  }

  record CacheValue(Table table) {
  }
}