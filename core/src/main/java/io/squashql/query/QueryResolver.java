package io.squashql.query;

import io.squashql.query.compiled.*;
import io.squashql.query.dto.*;
import io.squashql.query.exception.FieldNotFoundException;
import io.squashql.store.Store;
import io.squashql.type.FunctionTypedField;
import io.squashql.type.TableTypedField;
import io.squashql.type.TypedField;
import lombok.Value;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Value
//todo-mde rename to compiled query
public class QueryResolver {
  // todo-mde remove storesByName
  Map<String, Store> storesByName;
  QueryExecutor.QueryScope scope;
  List<TypedField> bucketColumns;
  List<TypedField> columns;
  CompilationCache cache;

  public QueryResolver(QueryDto query, Map<String, Store> storesByName) {
    this.storesByName = storesByName;
    if (query.virtualTableDto != null) {
      this.storesByName.put(query.virtualTableDto.name, VirtualTableDto.toStore(query.virtualTableDto));
    }
    this.cache = new CompilationCache();
    this.columns = query.columns.stream().map(this::resolveField).toList();
    this.bucketColumns = Optional.ofNullable(query.columnSets.get(ColumnSetKey.BUCKET))
            .stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()).map(this::resolveField).toList();
    this.scope = toQueryScope(query);
  }

  /** Filed resolver */
  private TypedField resolveField(final Field field) {
    return this.cache.computeIfAbsent(field, f -> {
      if (f instanceof TableField tf) {
        return getTableTypedField(tf.name());
      } else if (f instanceof FunctionField ff) {
        return new FunctionTypedField(getTableTypedField(ff.field.name()), ff.function);
      } else {
        throw new IllegalArgumentException(f.getClass().getName());
      }
    });
  }

  private TypedField resolveWithFallback(Field field) {
    try {
      return resolveField(field);
    } catch (FieldNotFoundException e) {
      // This can happen if the using a "field" coming from the calculation of a subquery. Since the field provider
      // contains only "raw" fields, it will throw an exception.
      return new TableTypedField(null, field.name(), Number.class);
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

  /** Queries */
  private QueryExecutor.QueryScope toQueryScope(final QueryDto query) {
    checkQuery(query);
    final List<TypedField> columnSets = query.columnSets.values().stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()).map(this::resolveField).toList();
    final List<TypedField> combinedColumns = Stream.concat(this.columns.stream(), columnSets.stream()).toList();

    List<TypedField> rollupColumns = query.rollupColumns.stream().map(this::resolveField).toList();
    List<List<TypedField>> groupingSets = query.groupingSets.stream().map(g -> g.stream().map(this::resolveField).toList()).toList();
    return new QueryExecutor.QueryScope(compileTable(query.table),
            query.subQuery == null ? null : toSubQuery(query.subQuery),
            combinedColumns,
            compileMeasures(query.measures),
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
            compileMeasures(subQuery.measures),
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
  }

  public DatabaseQuery2 toDatabaseQuery(final QueryExecutor.QueryScope query, final int limit) {
    return new DatabaseQuery2(query.virtualTable(),
            query.table(),
            query.subQuery() == null ? null : toSubQuery(query.subQuery()),
            query.columns(),
            query.whereCriteria(),
            query.havingCriteria(),
            new HashSet<>(query.measures()),
            query.rollupColumns(),
            query.groupingSets(),
            limit);
  }

  private DatabaseQuery2 toSubQuery(final QueryExecutor.QueryScope subQuery) {
    return new DatabaseQuery2(subQuery.virtualTable(),
            subQuery.table(),
            null,
            subQuery.columns(),
            subQuery.whereCriteria(),
            subQuery.havingCriteria(),
            new HashSet<>(subQuery.measures()),
            subQuery.rollupColumns(),
            subQuery.groupingSets(),
            -1);
  }

  /** Table */
  private CompiledTable compileTable(final TableDto table) {
    return new CompiledTable(table.name, table.joins.stream().map(this::compileJoin).collect(Collectors.toList()));
  }

  private CompiledTable.CompiledJoin compileJoin(JoinDto join) {
    return new CompiledTable.CompiledJoin(compileTable(join.table), join.type, compileCriteria(join.joinCriteria));
  }

  /** Criteria */
  private CompiledCriteria compileCriteria(final CriteriaDto criteria) {
    return criteria == null
            ? null
            : this.cache.computeIfAbsent(criteria, c -> new CompiledCriteria(c, c.field == null ? null : resolveWithFallback(c.field), c.fieldOther == null ? null : resolveWithFallback(c.fieldOther),
                    c.measure == null ? null : compileMeasure(c.measure),
                    c.children.stream().map(this::compileCriteria).collect(Collectors.toList())));
  }

  /** measures */
  private List<CompiledMeasure> compileMeasures(final List<Measure> measures) {
    return measures.stream().map(this::compileMeasure).collect(Collectors.toList());
  }

  private CompiledMeasure compileMeasure(Measure measure) {
    return this.cache.computeIfAbsent(measure, m -> {
      final CompiledMeasure compiledMeasure;
      if (m instanceof AggregatedMeasure) {
        compiledMeasure = compileAggregatedMeasure((AggregatedMeasure) m);
      } else if (m instanceof ExpressionMeasure) {
        compiledMeasure = compileExpressionMeasure((ExpressionMeasure) m);
      } else if (m instanceof ConstantMeasure<?>) {
        compiledMeasure = compileConstantMeasure((ConstantMeasure<?>) m);
      } else if (m instanceof BinaryOperationMeasure) {
        compiledMeasure = compileBinaryOperationMeasure((BinaryOperationMeasure) m);
      } else if (m instanceof ComparisonMeasureReferencePosition) {
        compiledMeasure = compileComparisonMeasure((ComparisonMeasureReferencePosition) m);
      } else {
        throw new IllegalArgumentException("Unknown type of measure " + m.getClass().getSimpleName());
      }
      if (compiledMeasure.accept(new PrimitiveMeasureVisitor())) {
        return compiledMeasure;
      }
      throw new IllegalArgumentException("Only measures that can be computed by the underlying database can be used" +
              " in a sub-query but " + m + " was provided");
    });
  }

  private CompiledMeasure compileAggregatedMeasure(AggregatedMeasure m) {
    return new CompiledAggregatedMeasure(m, resolveField(m.field), compileCriteria(m.criteria));
  }

  private CompiledMeasure compileExpressionMeasure(ExpressionMeasure m) {
    return new CompiledExpressionMeasure(m);
  }

  private CompiledMeasure compileConstantMeasure(ConstantMeasure<?> m) {
    return new CompiledConstantMeasure(m);
  }

  private CompiledMeasure compileBinaryOperationMeasure(BinaryOperationMeasure m) {
    return new CompiledBinaryOperationMeasure(m, compileMeasure(m.leftOperand), compileMeasure(m.rightOperand));
  }

  private CompiledMeasure compileComparisonMeasure(ComparisonMeasureReferencePosition m) {
    return new CompiledComparisonMeasure(m, compileMeasure(m.measure),
            m.referencePosition.entrySet().stream().collect(Collectors.toMap(e -> resolveField(e.getKey()), Map.Entry::getValue)),
            compilePeriod(m.period),
            m.ancestors.stream().map(this::resolveField).collect(Collectors.toList()));
  }

  private CompiledPeriod compilePeriod(Period period) {
    final List<TypedField> compiledFileds = period.getFields().stream().map(this::resolveField).toList();
    if (period instanceof Period.Month month) {
      return new CompiledPeriod.Month(resolveField(month.month()), resolveField(month.year()));
    } else if (period instanceof Period.Quarter quarter) {
      return new CompiledPeriod.Quarter(resolveField(quarter.quarter()), resolveField(quarter.year()));
    } else if (period instanceof Period.Semester semester) {
      return new CompiledPeriod.Semester(resolveField(semester.semester()), resolveField(semester.year()));
    } else if (period instanceof Period.Year year) {
      return new CompiledPeriod.Year(resolveField(year.year()));
    } else {
      throw new IllegalArgumentException("Unknown Period type " + period.getClass().getSimpleName());
    }
  }

  @Value
  private static class CompilationCache {
    Map<Field, TypedField> compiledFields = new ConcurrentHashMap<>();
    Map<Measure, CompiledMeasure> compiledMeasure = new ConcurrentHashMap<>();
    Map<CriteriaDto, CompiledCriteria> compiledCriteria = new ConcurrentHashMap<>();

    /** We don't rely on Map.computeIfAbsent directly as it doesn't allow recursive updates */
    private TypedField computeIfAbsent(final Field field, final Function<Field, TypedField> mappingFunction) {
      if (this.compiledFields.containsKey(field)) {
        return this.compiledFields.get(field);
      } else {
        final TypedField typedField = mappingFunction.apply(field);
        this.compiledFields.put(field, typedField);
        return typedField;
      }
    }

    private CompiledMeasure computeIfAbsent(final Measure measure, final Function<Measure, CompiledMeasure> mappingFunction) {
      if (this.compiledMeasure.containsKey(measure)) {
        return this.compiledMeasure.get(measure);
      } else {
        final CompiledMeasure compiledMeasure = mappingFunction.apply(measure);
        this.compiledMeasure.put(measure, compiledMeasure);
        return compiledMeasure;
      }
    }

    private CompiledCriteria computeIfAbsent(final CriteriaDto criteria, final Function<CriteriaDto, CompiledCriteria> mappingFunction) {
      if (this.compiledCriteria.containsKey(criteria)) {
        return this.compiledCriteria.get(criteria);
      } else {
        final CompiledCriteria compiledCriteria = mappingFunction.apply(criteria);
        this.compiledCriteria.computeIfAbsent(criteria, mappingFunction);
        return compiledCriteria;
      }
    }
  }
}
