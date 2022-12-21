package me.paulbares.spring.web.rest;

import com.google.common.collect.ImmutableList;
import me.paulbares.query.*;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.dto.*;
import me.paulbares.query.monitoring.QueryWatch;
import me.paulbares.store.Field;
import me.paulbares.store.Store;
import org.springframework.core.env.Environment;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

@RestController
public class QueryController {

  public static final String MAPPING_QUERY = "/query";
  public static final String MAPPING_QUERY_BEAUTIFY = "/query-beautify";
  public static final String MAPPING_METADATA = "/metadata";
  public static final String MAPPING_EXPRESSION = "/expression";
  protected final QueryEngine queryEngine;
  protected final QueryExecutor queryExecutor;

  public QueryController(QueryEngine queryEngine, Environment environment) {
    this.queryEngine = queryEngine;
    this.queryExecutor = new QueryExecutor(this.queryEngine);
  }

  @PostMapping(MAPPING_QUERY)
  public ResponseEntity<QueryResultDto> execute(@RequestBody QueryDto query) throws IllegalAccessException {
    QueryWatch queryWatch = new QueryWatch();
    CacheStatsDto.CacheStatsDtoBuilder csBuilder = CacheStatsDto.builder();
    Table table = this.queryExecutor.execute(query, queryWatch, csBuilder);
    List<String> fields = table.headers().stream().map(Field::name).collect(Collectors.toList());
    SimpleTableDto simpleTable = SimpleTableDto.builder()
            .rows(ImmutableList.copyOf(table.iterator()))
            .columns(fields)
            .build();
    QueryResultDto result = QueryResultDto.builder()
            .table(simpleTable)
            .metadata(TableUtils.buildTableMetadata(table))
            .debug(DebugInfoDto.builder()
                    .cache(csBuilder.build())
                    .timings(queryWatch.toQueryTimings()).build())
            .build();
    return ResponseEntity.ok(result);
  }

  @PostMapping(MAPPING_QUERY_BEAUTIFY)
  public ResponseEntity<String> executeBeautify(String apiKey, @RequestBody QueryDto query) throws IllegalAccessException {
    Table table = this.queryExecutor.execute(query);
    return ResponseEntity.ok(table.toString());
  }

  @GetMapping(MAPPING_METADATA)
  public ResponseEntity<MetadataResultDto> getMetadata(String apiKey) throws IllegalAccessException {
    List<MetadataResultDto.StoreMetadata> stores = new ArrayList<>();
    for (Store store : this.queryEngine.datastore().storesByName().values()) {
      List<MetadataItem> items = store.fields().stream().map(f -> new MetadataItem(f.name(), f.name(), f.type())).toList();
      stores.add(new MetadataResultDto.StoreMetadata(store.name(), items));
    }
    return ResponseEntity.ok(new MetadataResultDto(stores, this.queryEngine.supportedAggregationFunctions(), Collections.emptyList()));
  }

  @PostMapping(MAPPING_EXPRESSION)
  public ResponseEntity<List<Measure>> setMeasureExpressions(@RequestBody List<Measure> measures) {
    List<Measure> res = new ArrayList<>(measures);
    for (Measure measure : res) {
      String expression = measure.expression();
      if (expression == null) {
        measure.setExpression(MeasureUtils.createExpression(measure));
      }
    }
    return ResponseEntity.ok(res);
  }
}
