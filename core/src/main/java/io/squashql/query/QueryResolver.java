package io.squashql.query;

import io.squashql.PrimitiveMeasureVisitor;
import io.squashql.query.compiled.CompiledCriteria;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.compiled.CompiledTable;
import io.squashql.query.compiled.DatabaseQuery2;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.dto.CriteriaDto;
import io.squashql.query.dto.QueryDto;
import io.squashql.query.dto.TableDto;
import io.squashql.query.exception.FieldNotFoundException;
import io.squashql.store.Store;
import io.squashql.type.FunctionTypedField;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import lombok.Value;

import java.util.*;
import java.util.function.Function;

@Value
public class QueryResolver {

  Map<String, Store> storesByName;


//  public static DatabaseQuery queryScopeToDatabaseQuery(QueryExecutor.QueryScope queryScope, QueryResolver queryResolver, int limit) {
//    Set<TypedField> selects = new HashSet<>(queryScope.columns());
//    DatabaseQuery prefetchQuery = new DatabaseQuery();
//    if (queryScope.tableDto() != null) {
//      prefetchQuery.table(queryScope.tableDto());
//    } else if (queryScope.subQuery() != null) {
//      prefetchQuery.subQuery(toSubDatabaseQuery(queryScope.subQuery(), fieldSupplier));
//    } else {
//      throw new IllegalArgumentException("A table or sub-query was expected in " + queryScope);
//    }
//    prefetchQuery.whereCriteria(queryScope.whereCriteriaDto());
//    prefetchQuery.havingCriteria(queryScope.havingCriteriaDto());
//    selects.forEach(prefetchQuery::withSelect);
//    prefetchQuery.rollup(queryScope.rollupColumns());
//    prefetchQuery.groupingSets(queryScope.groupingSets());
//    prefetchQuery.limit(limit);
//    prefetchQuery.virtualTable(queryScope.virtualTableDto());
//    return prefetchQuery;
//  }
//
//  public static DatabaseQuery toSubDatabaseQuery(QueryDto query, Function<Field, TypedField> fieldSupplier) {
//    if (query.subQuery != null) {
//      throw new IllegalArgumentException("sub-query in a sub-query is not supported");
//    }
//
//    if (query.virtualTableDto != null) {
//      throw new IllegalArgumentException("virtualTableDto in a sub-query is not supported");
//    }
//
//    Set<Field> cols = new HashSet<>(query.columns);
//    if (query.columnSets != null && !query.columnSets.isEmpty()) {
//      throw new IllegalArgumentException("column sets are not expected in sub query: " + query);
//    }
//    if (query.parameters != null && !query.parameters.isEmpty()) {
//      throw new IllegalArgumentException("parameters are not expected in sub query: " + query);
//    }
//
//    for (Measure measure : query.measures) {
//      if (measure.accept(new PrimitiveMeasureVisitor())) {
//        continue;
//      }
//      throw new IllegalArgumentException("Only measures that can be computed by the underlying database can be used" +
//              " in a sub-query but " + measure + " was provided");
//    }
//
//    DatabaseQuery prefetchQuery = new DatabaseQuery().table(query.table);
//    prefetchQuery.whereCriteriaDto = query.whereCriteriaDto;
//    prefetchQuery.havingCriteriaDto = query.havingCriteriaDto;
//    cols.stream().map(fieldSupplier).forEach(prefetchQuery::withSelect);
//    query.measures.forEach(prefetchQuery::withMeasure);
//    return prefetchQuery;
//  }

  public DatabaseQuery2 compileQuery(final QueryExecutor.QueryScope query, final List<Measure> queriedMeasures, final int limit) {
    checkQuery(query);
    final CompiledTable table;
    final DatabaseQuery2 subQuery;
    if (query.subQuery() == null) {
      subQuery = null;
      table = compileTable(query.tableDto());
    } else {
      subQuery = compileSubQuery(query.subQuery());
      table = null;
    }
    final List<TypedField> select = Collections.unmodifiableList(query.columns());
    final CompiledCriteria whereCriteria = compileCriteria(query.whereCriteriaDto());
    final CompiledCriteria havingCriteria = compileCriteria(query.havingCriteriaDto());
    final List<CompiledMeasure> measures = compileMeasure(queriedMeasures);
    final List<TypedField> rollup = Collections.unmodifiableList(query.rollupColumns());
    final List<List<TypedField>> groupingSets = Collections.unmodifiableList(query.groupingSets());
    return new DatabaseQuery2(query.virtualTableDto(),
            table,
            subQuery,
            select,
            whereCriteria,
            havingCriteria,
            measures,
            rollup,
            groupingSets,
            limit);

  }

  private void checkQuery(final QueryExecutor.QueryScope query) {
    if (query.tableDto() == null && query.subQuery() == null) {
      throw new IllegalArgumentException("A table or sub-query was expected in " + query);
    } else if (query.tableDto() != null && query.subQuery() != null) {
      throw new IllegalArgumentException("Cannot define a table and a sub-query at the same time in " + query);
    }
  }

  private DatabaseQuery2 compileSubQuery(final QueryDto subQuery) {
    checkSubQuery(subQuery);
    final CompiledTable table = compileTable(subQuery.table);
    final List<TypedField> select = subQuery.columns.stream().map(this::resolveField).toList();
    final CompiledCriteria whereCriteria = compileCriteria(subQuery.whereCriteriaDto);
    final CompiledCriteria havingCriteria = compileCriteria(subQuery.havingCriteriaDto);
    final List<CompiledMeasure> measures = compileMeasure(subQuery.measures);
    // should we check groupingSet and rollup as well are empty ?
    return new DatabaseQuery2(null,
            table,
            null,
            select,
            whereCriteria,
            havingCriteria,
            measures,
            Collections.emptyList(),
            Collections.emptyList(),
            -1);
  }

  // todo-mde maybe move this part when building the subQuery ?
  private void checkSubQuery(final QueryDto subQuery) {
    if (subQuery.subQuery != null) {
      throw new IllegalArgumentException("sub-query in a sub-query is not supported");
    }
    if (subQuery.virtualTableDto != null) {
      throw new IllegalArgumentException("virtualTableDto in a sub-query is not supported");
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

  private CompiledTable compileTable(final TableDto table) {
    return null;
  }

  private CompiledCriteria compileCriteria(final CriteriaDto whereCriteriaDto) {
    return null;
  }

  private List<CompiledMeasure> compileMeasure(final List<Measure> measures) {
    return null;
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
