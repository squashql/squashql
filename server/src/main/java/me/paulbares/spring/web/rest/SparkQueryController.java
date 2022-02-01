package me.paulbares.spring.web.rest;

import me.paulbares.SparkStore;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.ScenarioGroupingExecutor;
import me.paulbares.query.SparkQueryEngine;
import me.paulbares.query.Table;
import me.paulbares.query.dto.QueryDto;
import me.paulbares.query.dto.ScenarioGroupingQueryDto;
import me.paulbares.store.Store;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static me.paulbares.store.Datastore.SCENARIO_FIELD_NAME;

@RestController
public class SparkQueryController {

  public static final String MAPPING_QUERY = "/spark-query";
  public static final String MAPPING_QUERY_GROUPING = MAPPING_QUERY + "-scenario-grouping";
  public static final String MAPPING_METADATA = "/spark-metadata";

  public static final String METADATA_FIELDS_KEY = "fields";
  public static final String METADATA_STORES_KEY = "stores";
  public static final String METADATA_AGG_FUNC_KEY = "aggregation_functions";
  public static final List<String> SUPPORTED_AGG_FUNCS = List.of("sum", "min", "max", "avg", "var_samp", "var_pop", "stddev_samp", "stddev_pop", "count");

  protected final SparkQueryEngine queryEngine;

  public SparkQueryController(SparkQueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  @PostMapping(MAPPING_QUERY)
  public ResponseEntity<String> execute(@RequestBody QueryDto query) {
    Table table = this.queryEngine.execute(query);
    return ResponseEntity.ok(JacksonUtil.tableToCsv(table));
  }


  @PostMapping(MAPPING_QUERY_GROUPING)
  public ResponseEntity<String> executeGrouping(@RequestBody ScenarioGroupingQueryDto query) {
    Table table = new ScenarioGroupingExecutor(this.queryEngine).execute(query);
    return ResponseEntity.ok(JacksonUtil.tableToCsv(table));
  }

  @GetMapping(MAPPING_METADATA)
  public ResponseEntity<Map<Object, Object>> getMetadata() {
    List<Map<String, Object>> root = new ArrayList<>();
    for (Store store : this.queryEngine.datastore.stores()) {
      List<Map<String, String>> collect = store.getFields()
              .stream()
              .filter(f -> !f.name().equals(SparkStore.scenarioFieldName(store.name())))
              .map(f -> Map.of("name", f.name(), "type", f.type().getSimpleName().toLowerCase()))
              .collect(Collectors.toCollection(() -> new ArrayList<>()));
      collect.add(Map.of("name", SCENARIO_FIELD_NAME, "type", DataTypes.StringType.simpleString()));
      root.add(Map.of("name", store.name(), METADATA_FIELDS_KEY, collect));
    }
    return ResponseEntity.ok(Map.of(
            METADATA_STORES_KEY, root,
            METADATA_AGG_FUNC_KEY, SUPPORTED_AGG_FUNCS));
  }
}
