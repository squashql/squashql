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

  public static final String HTTP_HEADER_API_KEY = "x-api-key";

  public static final String MAPPING_QUERY = "/query";
  public static final String MAPPING_QUERY_BEAUTIFY = "/query-beautify";
  public static final String MAPPING_METADATA = "/metadata";
  public static final String MAPPING_EXPRESSION = "/expression";

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

  protected final QueryEngine queryEngine;
  protected final QueryExecutor queryExecutor;
  protected final String apiKey;

  public QueryController(QueryEngine queryEngine, Environment environment) {
    this.queryEngine = queryEngine;
    this.apiKey = environment.getRequiredProperty(HTTP_HEADER_API_KEY);
    this.queryExecutor = new QueryExecutor(this.queryEngine);
  }

  @PostMapping(MAPPING_QUERY)
  public ResponseEntity<QueryResultDto> execute(@RequestHeader(HTTP_HEADER_API_KEY) String apiKey, @RequestBody QueryDto query) throws IllegalAccessException {
    checkApiKey(apiKey);
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
  public ResponseEntity<String> executeBeautify(@RequestHeader(HTTP_HEADER_API_KEY) String apiKey, @RequestBody QueryDto query) throws IllegalAccessException {
    checkApiKey(apiKey);
    Table table = this.queryExecutor.execute(query);
    return ResponseEntity.ok(table.toString());
  }

  @GetMapping(MAPPING_METADATA)
  public ResponseEntity<MetadataResultDto> getMetadata(@RequestHeader(HTTP_HEADER_API_KEY) String apiKey,
                                                       @RequestParam(name = "repo-url", required = false) String repo_url) throws IllegalAccessException {
    checkApiKey(apiKey);
    List<MetadataResultDto.StoreMetadata> stores = new ArrayList<>();
    for (Store store : this.queryEngine.datastore().storesByName().values()) {
      List<MetadataItem> items = store.fields().stream().map(f -> new MetadataItem(f.name(), f.name(), f.type())).toList();
      stores.add(new MetadataResultDto.StoreMetadata(store.name(), items));
    }

    return ResponseEntity.ok(new MetadataResultDto(stores, SUPPORTED_AGG_FUNCS, getExpressions(repo_url)));
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

  private List<Measure> getExpressions(String url) {
    if (url != null && !url.isEmpty()) {
      return new ArrayList<>(ExpressionResolver.get(url).values());
    } else {
      return Collections.emptyList();
    }
  }

  private void checkApiKey(String httpHeaderApiKey) throws IllegalAccessException {
    if (!this.apiKey.equals(httpHeaderApiKey)) {
      throw new IllegalAccessException();
    }
  }
}
