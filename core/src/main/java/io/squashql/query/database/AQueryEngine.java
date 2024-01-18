package io.squashql.query.database;

import io.squashql.query.Header;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.store.Datastore;
import io.squashql.table.Table;
import io.squashql.type.TypedField;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

@Slf4j
public abstract class AQueryEngine<T extends Datastore> implements QueryEngine<T> {

  public final T datastore;

  protected AQueryEngine(T datastore) {
    this.datastore = datastore;
  }

  @Override
  public T datastore() {
    return this.datastore;
  }

  @Override
  public Table execute(DatabaseQuery query) {
//    if (query.table != null) {
//      String tableName = query.table.name();
//      // Can be null if sub-query
//      Store store = this.datastore.storesByName().get(tableName);
//      if (store == null) {
//        throw new IllegalArgumentException(String.format("Cannot find table with name %s. Available tables: %s",
//                tableName, this.datastore.storesByName().values().stream().map(Store::name).toList()));
//      }
//    }
    String sql = createSqlStatement(query);
    log.info(query + " translated into " + System.lineSeparator() + "sql=" + sql);
    return retrieveAggregates(query, sql);
  }

  protected String createSqlStatement(DatabaseQuery query) {
    return SQLTranslator.translate(query, this.queryRewriter(query));
  }

  protected abstract Table retrieveAggregates(DatabaseQuery query, String sql);

  public static <Column, Record> Pair<List<Header>, List<List<Object>>> transformToColumnFormat(
          Collection<TypedField> typedFields,
          Collection<CompiledMeasure> measures,
          List<Column> columns,
          BiFunction<Column, String, Class<?>> columnTypeProvider,
          Iterator<Record> recordIterator,
          BiFunction<Integer, Record, Object> recordToFieldValue) {
    List<Header> headers = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>(typedFields.stream().map(SqlUtils::squashqlExpression).toList());
    measures.forEach(m -> fieldNames.add(m.alias()));
    List<List<Object>> values = new ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      headers.add(new Header(
              fieldNames.get(i),
              columnTypeProvider.apply(columns.get(i), fieldNames.get(i)),
              i >= typedFields.size()));
      values.add(new ArrayList<>());
    }
    recordIterator.forEachRemaining(r -> {
      for (int i = 0; i < headers.size(); i++) {
        values.get(i).add(recordToFieldValue.apply(i, r));
      }
    });
    return Tuples.pair(headers, values);
  }

  public static <Column, Record> Pair<List<Header>, List<List<Object>>> transformToRowFormat(
          List<Column> columns,
          Function<Column, String> columnNameProvider,
          Function<Column, Class<?>> columnTypeProvider,
          Iterator<Record> recordIterator,
          BiFunction<Integer, Record, Object> recordToFieldValue) {
    List<Header> headers = columns.stream().map(column -> new Header(columnNameProvider.apply(column), columnTypeProvider.apply(column), false)).toList();
    List<List<Object>> rows = new ArrayList<>();
    recordIterator.forEachRemaining(r -> rows.add(
            IntStream.range(0, headers.size()).mapToObj(i -> recordToFieldValue.apply(i, r)).toList()));
    return Tuples.pair(headers, rows);
  }
}
