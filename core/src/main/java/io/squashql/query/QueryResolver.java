package io.squashql.query;

import io.squashql.query.compiled.*;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.TableDto;
import io.squashql.query.dto.VirtualTableDto;
import io.squashql.query.exception.FieldNotFoundException;
import io.squashql.store.Store;
import io.squashql.type.FunctionTypedField;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import lombok.Value;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Stream;

@Value
//todo-mde rename to compiled query
public class QueryResolver {
  // todo-mde remove storesByName
  Map<String, Store> storesByName;
  QueryExecutor.QueryScope scope;
  List<CompiledMeasure> measures;
  List<TypedField> columnSets;
  List<TypedField> columns;

  public QueryResolver(QueryDto query, Map<String, Store> storesByName) {
    this.storesByName = storesByName;
    if (query.virtualTableDto != null) {
      this.storesByName.put(query.virtualTableDto.name, VirtualTableDto.toStore(query.virtualTableDto));
    }
    this.columns = query.columns.stream().map(this::resolveField).toList();
    this.columnSets = query.columnSets.values().stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()).map(this::resolveField).toList();
    final List<TypedField> combinedColumns = Stream.concat(columns.stream(), columnSets.stream()).toList();
    this.scope = toQueryScope(combinedColumns, query);
    this.measures = compileMeasure(query.measures);
  }

  private QueryExecutor.QueryScope toQueryScope(final List<TypedField> combinedColumns, final QueryDto query) {
    checkQuery(query);
    List<TypedField> rollupColumns = query.rollupColumns.stream().map(this::resolveField).toList();
    List<List<TypedField>> groupingSets = query.groupingSets.stream().map(g -> g.stream().map(this::resolveField).toList()).toList();
    return new QueryExecutor.QueryScope(compileTable(query.table),
            toSubQuery(query.subQuery),
            combinedColumns,
            compileCriteria(query.whereCriteriaDto),
            compileCriteria(query.havingCriteriaDto),
            rollupColumns,
            groupingSets,
            query.virtualTableDto);
  }

  private void checkQuery(final QueryDto query) {
    if (query.table == null && query.subQuery == null) {
      throw new IllegalArgumentException("A table or sub-query was expected in " + query);
    } else if (query.table != null && query.subQuery != null) {
      throw new IllegalArgumentException("Cannot define a table and a sub-query at the same time in " + query);
    }
  }

  private QueryExecutor.QueryScope toSubQuery(final QueryDto subQuery) {
    checkSubQuery(subQuery);
    final CompiledTable table = compileTable(subQuery.table);
    final List<TypedField> select = subQuery.columns.stream().map(this::resolveField).toList();
    final CompiledCriteria whereCriteria = compileCriteria(subQuery.whereCriteriaDto);
    final CompiledCriteria havingCriteria = compileCriteria(subQuery.havingCriteriaDto);
    // should we check groupingSet and rollup as well are empty ?
    return new QueryExecutor.QueryScope(table,
            null,
            select,
            whereCriteria,
            havingCriteria,
            Collections.emptyList(),
            Collections.emptyList(),
            null);
  }

  private void checkSubQuery(final QueryDto subQuery) {
    if (subQuery.subQuery != null) {
      throw new IllegalArgumentException("sub-query in a sub-query is not supported");
    }
    if (subQuery.virtualTableDto != null) {
      throw new IllegalArgumentException("virtualTable in a sub-query is not supported");
    }
    if (subQuery.columnSets != null && !subQuery.columnSets.isEmpty()) {
      throw new IllegalArgumentException("column sets are not expected in sub query: " + subQuery);
    }
    if (subQuery.parameters != null && !subQuery.parameters.isEmpty()) {
      throw new IllegalArgumentException("parameters are not expected in sub query: " + subQuery);
    }
    for (Measure measure : subQuery.measures) {
      if (measure.accept(new PrimitiveMeasureVisitor())) {
        continue;
      }
      throw new IllegalArgumentException("Only measures that can be computed by the underlying database can be used" +
              " in a sub-query but " + measure + " was provided");
    }
  }

  public DatabaseQuery2 toDatabaseQuery(final QueryExecutor.QueryScope query, final List<Measure> measures, final int limit) {
    return new DatabaseQuery2(query.virtualTable(),
            query.table(),
            toSubQuery(query.subQuery(), measures),
            query.columns(),
            query.whereCriteria(),
            query.havingCriteria(),
            Collections.emptyList(),
            query.rollupColumns(),
            query.groupingSets(),
            limit);
  }

  private DatabaseQuery2 toSubQuery(final QueryExecutor.QueryScope subQuery, final List<Measure> measures) {
    return new DatabaseQuery2(subQuery.virtualTable(),
            subQuery.table(),
            null,
            subQuery.columns(),
            subQuery.whereCriteria(),
            subQuery.havingCriteria(),
            Collections.emptyList(),
            subQuery.rollupColumns(),
            subQuery.groupingSets(),
            -1);
  }

  // todo-mde to implement
  private CompiledTable compileTable(final TableDto table) {
    return null;
  }

  private CompiledCriteria compileCriteria(final CriteriaDto whereCriteriaDto) {
    return null;
  }

  private List<CompiledMeasure> compileMeasure(final List<Measure> measures) {
    return null;
  }

  public static Function<Field, TypedField> withFallback(Function<Field, TypedField> fieldProvider, Class<?> fallbackType) {
    // todo-mde
    return field -> {
      try {
        return fieldProvider.apply(field);
      } catch (FieldNotFoundException e) {
        // This can happen if the using a "field" coming from the calculation of a subquery. Since the field provider
        // contains only "raw" fields, it will throw an exception.
        return new TableTypedField(null, field.name(), fallbackType);
      }
    };
  }

  public TypedField resolveField(final Field field) {
    if (field instanceof TableField tf) {
      return getTableTypedField(tf.name());
    } else if (field instanceof FunctionField ff) {
      return new FunctionTypedField(getTableTypedField(ff.field.name()), ff.function);
    } else {
      throw new IllegalArgumentException(field.getClass().getName());
    }
  }

  private TableTypedField getTableTypedField(String fieldName) {
    final String[] split = fieldName.split("\\.");
    if (split.length > 1) {
      final String tableName = split[0];
      final String fieldNameInTable = split[1];
      Store store = this.storesByName.get(tableName);
      if (store != null) {
        for (TableTypedField field : store.fields()) {
          if (field.name().equals(fieldNameInTable)) {
            return field;
          }
        }
      }
    } else {
      for (Store store : this.storesByName.values()) {
        for (TableTypedField field : store.fields()) {
          if (field.name().equals(fieldName)) {
            // We omit on purpose the store name. It will be determined by the underlying SQL engine of the DB.
            // if any ambiguity, the DB will raise an exception.
            return new TableTypedField(null, fieldName, field.type());
          }
        }
      }
    }

    if (fieldName.equals(CountMeasure.INSTANCE.alias())) {
      return new TableTypedField(null, CountMeasure.INSTANCE.alias(), long.class);
    }
    throw new FieldNotFoundException("Cannot find field with name " + fieldName);
  }

}
