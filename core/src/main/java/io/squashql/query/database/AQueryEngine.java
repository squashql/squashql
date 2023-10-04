package io.squashql.query.database;

import io.squashql.query.*;
import io.squashql.query.exception.FieldNotFoundException;
import io.squashql.store.Datastore;
import io.squashql.store.Store;
import io.squashql.table.ColumnarTable;
import io.squashql.table.Table;
import io.squashql.type.*;
import io.squashql.util.Queries;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.IntStream;

@Slf4j
public abstract class AQueryEngine<T extends Datastore> implements QueryEngine<T> {

  public final T datastore;

  protected final QueryRewriter queryRewriter;

  protected AQueryEngine(T datastore, QueryRewriter queryRewriter) {
    this.datastore = datastore;
    this.queryRewriter = queryRewriter;
  }

  @Override
  public QueryRewriter queryRewriter() {
    return this.queryRewriter;
  }

  public static Function<Field, TypedField> createFieldSupplier(Map<String, Store> storesByName) {
    return field -> getTypedField(field, storesByName);
  }

  private static TypedField getTypedField(Field field, Map<String, Store> storesByName) {
    if (field instanceof TableField tf) {
      return getTableTypedField(tf.name(), storesByName, field.alias());
    } else if (field instanceof FunctionField ff) {
      return new FunctionTypedField(getTableTypedField(ff.field.name(), storesByName, ff.field.alias()), ff.function, field.alias());
    } else if (field instanceof ConstantField cf) {
      return new ConstantTypedField(cf.value, cf.alias);
    } else if (field instanceof BinaryOperationField bof) {
      TypedField left = getTypedField(bof.leftOperand, storesByName);
      TypedField right = getTypedField(bof.rightOperand, storesByName);
      return new BinaryOperationTypedField(bof.operator, left, right, bof.alias);
    } else {
      throw new IllegalArgumentException(field.getClass().getName());
    }
  }

  private static TableTypedField getTableTypedField(String fieldName, Map<String, Store> storesByName, String alias) {
    String[] split = fieldName.split("\\.");
    if (split.length > 1) {
      String tableName = split[0];
      String fieldNameInTable = split[1];
      Store store = storesByName.get(tableName);
      if (store != null) {
        for (TableTypedField field : store.fields()) {
          if (field.getName().equals(fieldNameInTable)) {
            return new TableTypedField(field.store, field.name, field.type, alias);
          }
        }
      }
    } else {
      for (Store store : storesByName.values()) {
        for (TableTypedField field : store.fields()) {
          if (field.getName().equals(fieldName)) {
            // We omit on purpose the store name. It will be determined by the underlying SQL engine of the DB.
            // if any ambiguity, the DB will raise an exception.
            return new TableTypedField(null, fieldName, field.type(), alias);
          }
        }
      }
    }

    if (fieldName.equals(CountMeasure.INSTANCE.alias())) {
      return new TableTypedField(null, CountMeasure.INSTANCE.alias(), long.class);
    }
    throw new FieldNotFoundException("Cannot find field with name " + fieldName);
  }

  @Override
  public T datastore() {
    return this.datastore;
  }

  protected abstract Table retrieveAggregates(DatabaseQuery query, String sql);

  @Override
  public Table execute(DatabaseQuery query, QueryExecutor.PivotTableContext context) {
    if (query.table != null) {
      String tableName = query.table.name;
      // Can be null if sub-query
      Store store = this.datastore.storesByName().get(tableName);
      if (store == null) {
        throw new IllegalArgumentException(String.format("Cannot find table with name %s. Available tables: %s",
                tableName, this.datastore.storesByName().values().stream().map(Store::name).toList()));
      }
    }
    String sql = createSqlStatement(query, context);
    log.info(query + " translated into " + System.lineSeparator() + "sql=" + sql);
    Table aggregates = retrieveAggregates(query, sql);
    return postProcessDataset(aggregates, query);
  }

  protected String createSqlStatement(DatabaseQuery query, QueryExecutor.PivotTableContext context) {
    return SQLTranslator.translate(query,
            QueryExecutor.createQueryFieldSupplier(this, query.virtualTableDto),
            this.queryRewriter);
  }

  /**
   * Changes the content of the input table to remove columns corresponding to grouping() (columns that help to identify
   * rows containing totals) and write {@link SQLTranslator#TOTAL_CELL} in the corresponding cells. The modifications
   * happen in-place i.e. in the input table columns directly.
   * <pre>
   *   Input:
   *   +----------+----------+---------------------------+---------------------------+------+----------------------+----+
   *   | scenario | category | ___grouping___scenario___ | ___grouping___category___ |    p | _contributors_count_ |  q |
   *   +----------+----------+---------------------------+---------------------------+------+----------------------+----+
   *   |     base |    drink |                         0 |                         0 |  2.0 |                    1 | 10 |
   *   |     base |     food |                         0 |                         0 |  3.0 |                    1 | 20 |
   *   |     base |    cloth |                         0 |                         0 | 10.0 |                    1 |  3 |
   *   |     null |     null |                         1 |                         1 | 15.0 |                    3 | 33 |
   *   |     base |     null |                         0 |                         1 | 15.0 |                    3 | 33 |
   *   +----------+----------+---------------------------+---------------------------+------+----------------------+----+
   *   Output:
   *   +-------------+-------------+------+----------------------+----+
   *   |    scenario |    category |    p | _contributors_count_ |  q |
   *   +-------------+-------------+------+----------------------+----+
   *   |        base |       drink |  2.0 |                    1 | 10 |
   *   |        base |        food |  3.0 |                    1 | 20 |
   *   |        base |       cloth | 10.0 |                    1 |  3 |
   *   | ___total___ | ___total___ | 15.0 |                    3 | 33 |
   *   |        base | ___total___ | 15.0 |                    3 | 33 |
   *   +-------------+-------------+------+----------------------+----+
   * </pre>
   */
  protected Table postProcessDataset(Table input, DatabaseQuery query) {
    List<TypedField> groupingSelects = Queries.generateGroupingSelect(query);
    if (this.queryRewriter.useGroupingFunction() && !groupingSelects.isEmpty()) {
      List<Header> newHeaders = new ArrayList<>();
      List<List<Object>> newValues = new ArrayList<>();
      for (int i = 0; i < input.headers().size(); i++) {
        Header header = input.headers().get(i);
        List<Object> columnValues = input.getColumn(i);
        if (i < query.select.size() || i >= query.select.size() + groupingSelects.size()) {
          newHeaders.add(header);
          newValues.add(columnValues);
        } else {
          // FIXME this is dirty, we should be able to do better since are choosing the header names. Maybe we need to
          // use the TypedField in Header ?
          //          String baseName = Objects.requireNonNull(SqlUtils.extractFieldFromGroupingAlias(header.name()));
          Map<String, TypedField> m = new HashMap<>();
          for (TypedField typedField : groupingSelects) {
            m.put(String.format("grouping(%s)", this.queryRewriter.select(typedField)), typedField);
          }
          List<Object> baseColumnValues = input.getColumn(query.select.indexOf(m.get(header.name())));
          for (int rowIndex = 0; rowIndex < columnValues.size(); rowIndex++) {
            if (((Number) columnValues.get(rowIndex)).longValue() == 1) {
              // It is a total if == 1. It is cast as Number because the type is Byte with Spark, Long with
              // ClickHouse...
              baseColumnValues.set(rowIndex, SQLTranslator.TOTAL_CELL);
            }
          }
        }
      }

      return new ColumnarTable(
              newHeaders,
              input.measures(),
              newValues);
    } else {
      return input;
    }
  }

  public static <Column, Record> Pair<List<Header>, List<List<Object>>> transformToColumnFormat(
          DatabaseQuery query,
          List<Column> columns,
          BiFunction<Column, String, String> columnNameProvider,
          BiFunction<Column, String, Class<?>> columnTypeProvider,
          Iterator<Record> recordIterator,
          BiFunction<Integer, Record, Object> recordToFieldValue,
          QueryRewriter queryRewriter) {
    List<Header> headers = new ArrayList<>();
    List<String> fieldNames = new ArrayList<>(query.select.stream().map(s -> s.alias() != null ? s.alias() : getTypedFieldString(s)).toList());
    List<TypedField> groupingSelects = Queries.generateGroupingSelect(query);
    if (queryRewriter.useGroupingFunction()) {
      groupingSelects.forEach(r -> fieldNames.add(String.format("grouping(%s)", queryRewriter.select(r))));
//      groupingSelects.forEach(r -> fieldNames.add(SqlUtils.groupingAlias(getTypedFieldString(r))));
    }
    query.measures.forEach(m -> fieldNames.add(m.alias()));
    List<List<Object>> values = new ArrayList<>(columns.size());
    for (int i = 0; i < columns.size(); i++) {
      headers.add(new Header(
              columnNameProvider.apply(columns.get(i), fieldNames.get(i)),
              columnTypeProvider.apply(columns.get(i), fieldNames.get(i)),
              i >= query.select.size() + (queryRewriter.useGroupingFunction() ? groupingSelects.size() : 0)));
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

  private static String getTypedFieldString(TypedField field) {
    if (field instanceof TableTypedField ttf) {
      return SqlUtils.getFieldFullName(ttf);
    } else if (field instanceof FunctionTypedField ftf) {
      return SqlUtils.singleOperandFunctionName(ftf.function(), SqlUtils.getFieldFullName(ftf.field()));
    } else if (field instanceof ConstantTypedField ctf) {
      return Objects.toString(ctf.value);
    } else if (field instanceof BinaryOperationTypedField botf) {
      return new StringBuilder()
              .append("(")
              .append(getTypedFieldString(botf.leftOperand))
              .append(botf.operator.infix)
              .append(getTypedFieldString(botf.rightOperand))
              .append(")")
              .toString();
    } else {
      throw new IllegalArgumentException(field.getClass().getName());
    }
  }
}
