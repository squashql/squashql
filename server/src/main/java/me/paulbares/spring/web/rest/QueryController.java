package me.paulbares.spring.web.rest;

import me.paulbares.query.*;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.database.SparkQueryEngine;
import me.paulbares.query.dto.CacheStatsDto;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.monitoring.QueryWatch;
import me.paulbares.store.Store;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

import static me.paulbares.query.TableUtils.NAME_KEY;
import static me.paulbares.query.TableUtils.TYPE_KEY;

@RestController
public class QueryController {

  public static final String MAPPING_QUERY = "/spark-query";
  public static final String MAPPING_QUERY_BEAUTIFY = "/spark-query-beautify";
  public static final String MAPPING_METADATA = "/spark-metadata";

  public static final String METADATA_FIELDS_KEY = "fields";
  public static final String METADATA_STORES_KEY = "stores";
  public static final String METADATA_AGG_FUNCS_KEY = "aggregation_functions";
  public static final String METADATA_MEASURES_KEY = "measures";
  // FIXME the list should be defined elsewhere
  public static final List<String> SUPPORTED_AGG_FUNCS = List.of(
          "sum",
          "min",
          "max",
          "avg",
          "var_samp",
          "var_pop",
          "stddev_samp",
          "stddev_pop",
          "count");

  protected final SparkQueryEngine itmQueryEngine;

  public QueryController(SparkQueryEngine itmQueryEngine) {
    this.itmQueryEngine = itmQueryEngine;
  }

  @PostMapping(MAPPING_QUERY)
  public ResponseEntity<Map<String, Object>> execute(@RequestBody QueryDto query) {
    QueryWatch queryWatch = new QueryWatch();
    CacheStatsDto.CacheStatsDtoBuilder builder = CacheStatsDto.builder();
    Table table = new QueryExecutor(this.itmQueryEngine).execute(query, queryWatch, builder);
    Map<String, Object> result = Map.of(
            "table", JacksonUtil.serializeTable(table),
            "metadata", TableUtils.buildTableMetadata(table),
            "debug", Map.of("timings", queryWatch.toJson(), "cache", builder.build()));
    return ResponseEntity.ok(result);
  }

  @PostMapping(MAPPING_QUERY_BEAUTIFY)
  public ResponseEntity<String> executeBeautify(@RequestBody QueryDto query) {
    Table table = new QueryExecutor(this.itmQueryEngine).execute(query);
    return ResponseEntity.ok(table.toString());
  }

  @GetMapping(MAPPING_METADATA)
  public ResponseEntity<Map<Object, Object>> getMetadata(@RequestParam(name = "repo-url", required = false) String repo_url) {
    List<Map<String, Object>> root = new ArrayList<>();
    for (Store store : this.itmQueryEngine.datastore.storesByName().values()) {
      List<Map<String, String>> collect = store.fields()
              .stream()
              .map(f -> Map.of(NAME_KEY, f.name(), TYPE_KEY, f.type().getSimpleName().toLowerCase()))
              .toList();
      root.add(Map.of(NAME_KEY, store.name(), METADATA_FIELDS_KEY, collect));
    }

    return ResponseEntity.ok(Map.of(
            METADATA_STORES_KEY, root,
            METADATA_AGG_FUNCS_KEY, SUPPORTED_AGG_FUNCS,
            METADATA_MEASURES_KEY, getExpressions(repo_url)));
  }

  private Collection<ExpressionMeasure> getExpressions(String url) {
    if (url != null && !url.isEmpty()) {
      return ExpressionResolver.get(url).values();
    } else {
      return Collections.emptyList();
    }
  }
}
