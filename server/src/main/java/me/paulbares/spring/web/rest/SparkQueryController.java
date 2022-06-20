package me.paulbares.spring.web.rest;

import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.*;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.ScenarioGroupingQueryDto;
import me.paulbares.store.Store;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

@RestController
public class SparkQueryController {

  public static final String MAPPING_QUERY = "/spark-query";
  public static final String MAPPING_QUERY_GROUPING = MAPPING_QUERY + "-scenario-grouping";
  public static final String MAPPING_METADATA = "/spark-metadata";

  public static final String METADATA_FIELDS_KEY = "fields";
  public static final String METADATA_STORES_KEY = "stores";
  public static final String METADATA_AGG_FUNCS_KEY = "aggregation_functions";
  public static final String METADATA_METRICS_KEY = "metrics";
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

  public SparkQueryController(SparkQueryEngine itmQueryEngine) {
    this.itmQueryEngine = itmQueryEngine;
  }

  @PostMapping(MAPPING_QUERY)
  public ResponseEntity<String> execute(@RequestBody QueryDto query) {
    Table table = this.itmQueryEngine.execute(query);
    return ResponseEntity.ok(JacksonUtil.tableToCsv(table));
  }


  @PostMapping(MAPPING_QUERY_GROUPING)
  public ResponseEntity<String> executeGrouping(@RequestBody ScenarioGroupingQueryDto query) {
    Table table = new ScenarioGroupingExecutor(this.itmQueryEngine).execute(query);
    return ResponseEntity.ok(JacksonUtil.tableToCsv(table));
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
            METADATA_METRICS_KEY, getExpressions(repo_url)));
  }

  private Collection<ExpressionMeasure> getExpressions(String url) {
    if (url != null && !url.isEmpty()) {
      return ExpressionResolver.get(url).values();
    } else {
      return Collections.emptyList();
    }
  }
}
