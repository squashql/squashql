package io.squashql.spring.web.rest;

import io.squashql.query.*;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.dto.*;
import io.squashql.store.Store;
import io.squashql.table.PivotTable;
import io.squashql.table.PivotTableUtils;
import io.squashql.table.Renderable;
import io.squashql.table.Table;
import io.squashql.table.TableUtils;
import org.springframework.context.annotation.Import;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;

import java.util.*;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static io.squashql.query.QueryExecutor.createPivotTableContext;

@Import({JacksonConfiguration.class, SquashQLErrorHandler.class})
@RestController
public class QueryController {

  public static final String MAPPING_QUERY = "/query";
  public static final String MAPPING_QUERY_MERGE = "/query-merge";
  public static final String MAPPING_QUERY_JOIN_EXPERIMENTAL = "/experimental/query-join";
  public static final String MAPPING_QUERY_PIVOT = "/query-pivot";
  public static final String MAPPING_QUERY_MERGE_PIVOT = "/query-merge-pivot";
  public static final String MAPPING_METADATA = "/metadata";
  public static final String MAPPING_EXPRESSION = "/expression";
  protected final QueryEngine<?> queryEngine;
  public final QueryExecutor queryExecutor;
  protected final Supplier<SquashQLUser> squashQLUserSupplier;

  public QueryController(QueryEngine<?> queryEngine, Optional<Supplier<SquashQLUser>> squashQLUserSupplier) {
    this.queryEngine = queryEngine;
    this.queryExecutor = new QueryExecutor(this.queryEngine);
    this.squashQLUserSupplier = squashQLUserSupplier.orElse(null);
  }

  @PostMapping(MAPPING_QUERY)
  public ResponseEntity<?> execute(@RequestBody QueryDto query, @RequestHeader("Accept") String contentType) {
    CacheStatsDto.CacheStatsDtoBuilder csBuilder = CacheStatsDto.builder();
    Table table = this.queryExecutor.executeQuery(query,
            csBuilder,
            this.squashQLUserSupplier == null ? null : this.squashQLUserSupplier.get(),
            true,
            null,
            createPivotTableContext(query));
    return createResponseEntity(table, contentType, () -> createQueryResultDto(table, csBuilder, query.minify));
  }

  @PostMapping(MAPPING_QUERY_PIVOT)
  public ResponseEntity<?> execute(@RequestBody PivotTableQueryDto pivotTableQueryDto, @RequestHeader("Accept") String contentType) {
    CacheStatsDto.CacheStatsDtoBuilder csBuilder = CacheStatsDto.builder();
    PivotTable pt = this.queryExecutor.executePivotQuery(pivotTableQueryDto,
            csBuilder,
            this.squashQLUserSupplier == null ? null : this.squashQLUserSupplier.get(),
            true,
            null);
    List<Map<String, Object>> cells = PivotTableUtils.generateCells(pt, pivotTableQueryDto.query.minify);
    return createResponseEntity(pt, contentType, () -> new PivotTableQueryResultDto(cells, pt.rows, pt.columns, pt.values, pt.hiddenTotals));
  }

  @PostMapping(MAPPING_QUERY_MERGE)
  public ResponseEntity<?> executeAndMerge(@RequestBody QueryMergeDto queryMergeDto, @RequestHeader("Accept") String contentType) {
    Table table = this.queryExecutor.executeQueryMerge(
            queryMergeDto,
            this.squashQLUserSupplier == null ? null : this.squashQLUserSupplier.get());
    return createResponseEntity(table, contentType, () -> createQueryResultDto(table, CacheStatsDto.builder(), queryMergeDto.minify));
  }

  @PostMapping(MAPPING_QUERY_MERGE_PIVOT)
  public ResponseEntity<?> executeQueryMergePivot(@RequestBody PivotTableQueryMergeDto pivotTableQueryMergeDto, @RequestHeader("Accept") String contentType) {
    PivotTable pt = this.queryExecutor.executePivotQueryMerge(
            pivotTableQueryMergeDto,
            this.squashQLUserSupplier == null ? null : this.squashQLUserSupplier.get()
    );
    List<Map<String, Object>> cells = PivotTableUtils.generateCells(pt, pivotTableQueryMergeDto.query.minify);
    return createResponseEntity(pt, contentType, () -> new PivotTableQueryResultDto(cells, pt.rows, pt.columns, pt.values, pt.hiddenTotals));
  }

  @PostMapping(MAPPING_QUERY_JOIN_EXPERIMENTAL)
  public ResponseEntity<?> executeQueryJoin(@RequestBody QueryJoinDto queryJoinDto, @RequestHeader("Accept") String contentType) {
    Table table = this.queryExecutor.executeExperimentalQueryMerge(queryJoinDto);
    return createResponseEntity(table, contentType, () -> createQueryResultDto(table, CacheStatsDto.builder(), queryJoinDto.minify));
  }

  private static ResponseEntity<?> createResponseEntity(Renderable table, String contentType, Supplier<?> toJson) {
    switch (contentType) {
      case "application/json": return ResponseEntity.ok(toJson.get());
      case "text/csv": {
        return ResponseEntity.ok()
          .header("Content-Type", contentType)
          .header("Content-Disposition", "attachment; filename=\"result.csv\"")
          .body(table.toCSV());
      }
      default: return ResponseEntity.ok(table.toString());
    }
  }

  private static QueryResultDto createQueryResultDto(Table table, CacheStatsDto.CacheStatsDtoBuilder csBuilder, Boolean minify) {
    List<String> fields = table.headers().stream().map(Header::name).collect(Collectors.toList());
    QueryResultDto result = QueryResultDto.builder()
            .columns(fields)
            .cells(TableUtils.generateCells(table, minify))
            .metadata(TableUtils.buildTableMetadata(table))
            .debug(DebugInfoDto.builder().cache(csBuilder.build()).build())
            .build();
    return result;
  }

  @GetMapping(MAPPING_METADATA)
  public ResponseEntity<MetadataResultDto> getMetadata() {
    List<MetadataResultDto.StoreMetadata> stores = new ArrayList<>();
    for (Store store : this.queryEngine.datastore().storeByName().values()) {
      List<MetadataItem> items = store.fields().stream().map(f -> new MetadataItem(f.name(), f.name(), f.type())).toList();
      stores.add(new MetadataResultDto.StoreMetadata(store.name(), items));
    }
    return ResponseEntity.ok(new MetadataResultDto(stores, this.queryEngine.supportedAggregationFunctions(), Collections.emptyList()));
  }

  @PostMapping(MAPPING_EXPRESSION)
  public ResponseEntity<List<Measure>> setMeasureExpressions(@RequestBody List<Measure> measures) {
    List<Measure> res = new ArrayList<>(measures.size());
    for (Measure measure : measures) {
      String expression = measure.expression();
      Measure m = measure;
      if (expression == null) {
        m = measure.withExpression(MeasureUtils.createExpression(measure));
      }
      res.add(m);
    }
    return ResponseEntity.ok(res);
  }
}
