package me.paulbares.spring.web.rest;

import me.paulbares.query.QueryExecutor;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.ExpressionMeasure;
import me.paulbares.query.ExpressionResolver;
import me.paulbares.query.database.SparkQueryEngine;
import me.paulbares.query.Table;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.store.Store;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

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
  public ResponseEntity<String> execute(@RequestBody QueryDto query) {
    Table table = new QueryExecutor(this.itmQueryEngine).execute(query);
    return ResponseEntity.ok(JacksonUtil.tableToCsv(table));
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
              .map(f -> Map.of("name", f.name(), "type", f.type().getSimpleName().toLowerCase()))
              .toList();
      root.add(Map.of("name", store.name(), METADATA_FIELDS_KEY, collect));
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
