package io.squashql.spring.web.rest;

import com.google.common.collect.ImmutableList;
import io.squashql.query.*;
import io.squashql.query.database.QueryEngine;
import io.squashql.query.dto.*;
import io.squashql.store.Store;
import io.squashql.table.PivotTable;
import io.squashql.table.Table;
import io.squashql.table.TableUtils;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Collectors;

@RestController
public class QueryController {

  public static final String MAPPING_QUERY = "/query";
  public static final String MAPPING_QUERY_MERGE = "/query-merge";
  public static final String MAPPING_QUERY_PIVOT = "/query-pivot";
  public static final String MAPPING_QUERY_STRINGIFY = "/query-stringify";
  public static final String MAPPING_QUERY_PIVOT_STRINGIFY = "/query-pivot-stringify";
  public static final String MAPPING_METADATA = "/metadata";
  public static final String MAPPING_EXPRESSION = "/expression";
  protected final QueryEngine<?> queryEngine;
  protected final QueryExecutor queryExecutor;
  protected final Supplier<SquashQLUser> squashQLUserSupplier;

  public QueryController(QueryEngine<?> queryEngine, Optional<Supplier<SquashQLUser>> squashQLUserSupplier) {
    this.queryEngine = queryEngine;
    this.queryExecutor = new QueryExecutor(this.queryEngine);
    this.squashQLUserSupplier = squashQLUserSupplier.orElse(null);
  }

  @PostMapping(MAPPING_QUERY)
  public ResponseEntity<QueryResultDto> execute(@RequestBody QueryDto query) {
    CacheStatsDto.CacheStatsDtoBuilder csBuilder = CacheStatsDto.builder();
    Table table = this.queryExecutor.execute(query,
            null,
            csBuilder,
            this.squashQLUserSupplier == null ? null : this.squashQLUserSupplier.get(),
            true,
            null);
    List<String> fields = table.headers().stream().map(Header::name).collect(Collectors.toList());
    SimpleTableDto simpleTable = SimpleTableDto.builder()
            .rows(ImmutableList.copyOf(table.iterator()))
            .columns(fields)
            .build();
    QueryResultDto result = QueryResultDto.builder()
            .table(simpleTable)
            .metadata(TableUtils.buildTableMetadata(table))
            .debug(DebugInfoDto.builder().cache(csBuilder.build()).build())
            .build();
    return ResponseEntity.ok(result);
  }

  @PostMapping(MAPPING_QUERY_PIVOT)
  public ResponseEntity<PivotTableQueryResultDto> execute(@RequestBody PivotTableQueryDto pivotTableQueryDto) {
    CacheStatsDto.CacheStatsDtoBuilder csBuilder = CacheStatsDto.builder();
    PivotTable pt = this.queryExecutor.execute(pivotTableQueryDto,
            csBuilder,
            this.squashQLUserSupplier == null ? null : this.squashQLUserSupplier.get(),
            null);
    List<String> fields = pt.table.headers().stream().map(Header::name).collect(Collectors.toList());
    SimpleTableDto simpleTable = SimpleTableDto.builder()
            .rows(ImmutableList.copyOf(pt.table.iterator()))
            .columns(fields)
            .build();
    QueryResultDto result = QueryResultDto.builder()
            .table(simpleTable)
            .metadata(TableUtils.buildTableMetadata(pt.table))
            .debug(DebugInfoDto.builder().cache(csBuilder.build()).build())
            .build();
    return ResponseEntity.ok(new PivotTableQueryResultDto(result, pt.rows, pt.columns, pt.values));
  }

  @PostMapping(MAPPING_QUERY_MERGE)
  public ResponseEntity<QueryResultDto> executeAndMerge(@RequestBody QueryMergeDto queryMergeDto) {
    Table table = this.queryExecutor.execute(
            queryMergeDto.first,
            queryMergeDto.second,
            queryMergeDto.joinType,
            this.squashQLUserSupplier == null ? null : this.squashQLUserSupplier.get());
    List<String> fields = table.headers().stream().map(Header::name).collect(Collectors.toList());
    SimpleTableDto simpleTable = SimpleTableDto.builder()
            .rows(ImmutableList.copyOf(table.iterator()))
            .columns(fields)
            .build();
    QueryResultDto result = QueryResultDto.builder()
            .table(simpleTable)
            .metadata(TableUtils.buildTableMetadata(table))
            .build();
    return ResponseEntity.ok(result);
  }

  @PostMapping(MAPPING_QUERY_STRINGIFY)
  public ResponseEntity<String> executeStringify(@RequestBody QueryDto query) {
    Table table = this.queryExecutor.execute(query);
    return ResponseEntity.ok(table.toString());
  }

  @PostMapping(MAPPING_QUERY_PIVOT_STRINGIFY)
  public ResponseEntity<String> executePivotStringify(@RequestBody PivotTableQueryDto query) {
    PivotTable table = this.queryExecutor.execute(query);
    return ResponseEntity.ok(table.toString());
  }

  @GetMapping(MAPPING_METADATA)
  public ResponseEntity<MetadataResultDto> getMetadata() {
    List<MetadataResultDto.StoreMetadata> stores = new ArrayList<>();
    for (Store store : this.queryEngine.datastore().storesByName().values()) {
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
