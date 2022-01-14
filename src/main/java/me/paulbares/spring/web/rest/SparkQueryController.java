package me.paulbares.spring.web.rest;

import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.Query;
import me.paulbares.query.spark.SparkQueryEngine;
import me.paulbares.query.ScenarioGroupingQuery;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@RestController
public class SparkQueryController {

  public static final String MAPPING_QUERY = "/spark-query";
  public static final String MAPPING_QUERY_GROUPING = MAPPING_QUERY + "-scenario-grouping";
  public static final String MAPPING_METADATA = "/spark-metadata";

  public static final String METADATA_FIELDS_KEY = "fields";
  public static final String METADATA_AGG_FUNC_KEY = "aggregationFunctions";
  public static final List<String> SUPPORTED_AGG_FUNCS = List.of("sum", "min", "max", "avg", "var_samp", "var_pop", "stddev_samp", "stddev_pop", "count");

  protected final SparkQueryEngine queryEngine;

  public SparkQueryController(SparkQueryEngine queryEngine) {
    this.queryEngine = queryEngine;
  }

  @PostMapping(MAPPING_QUERY)
  public ResponseEntity<String> execute(@RequestBody Query query) {
    Dataset<Row> rowDataset = this.queryEngine.execute(query);
    return ResponseEntity.ok(JacksonUtil.datasetToCsv(rowDataset));
  }


  @PostMapping(MAPPING_QUERY_GROUPING)
  public ResponseEntity<String> executeGrouping(@RequestBody ScenarioGroupingQuery query) {
    Dataset<Row> rowDataset = this.queryEngine.executeGrouping(query);
    return ResponseEntity.ok(JacksonUtil.datasetToCsv(rowDataset));
  }

  @GetMapping(MAPPING_METADATA)
  public ResponseEntity<Map<Object, Object>> getMetadata() {
    List<Map<String, String>> collect = Arrays.stream(this.queryEngine.datastore.getFields())
            .map(f -> Map.of("name", f.name(), "type", f.dataType().simpleString()))
            .collect(Collectors.toCollection(() -> new ArrayList<>()));
    collect.add(Map.of("name", "scenario", "type", DataTypes.StringType.simpleString()));
    return ResponseEntity.ok(Map.of(
            METADATA_FIELDS_KEY, collect,
            METADATA_AGG_FUNC_KEY, SUPPORTED_AGG_FUNCS));
  }
}
