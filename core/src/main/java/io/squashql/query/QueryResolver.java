package io.squashql.query;

import io.squashql.query.compiled.*;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.dto.*;
import io.squashql.query.exception.FieldNotFoundException;
import io.squashql.store.Store;
import io.squashql.type.*;
import lombok.Data;
import lombok.Value;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.squashql.query.ColumnSetKey.BUCKET;
import static io.squashql.query.compiled.CompiledAggregatedMeasure.COMPILED_COUNT;

@Data
public class QueryResolver {

  private final QueryDto query;
  private final Map<String, Store> storesByName;
  private final QueryExecutor.QueryScope scope;
  private final List<TypedField> bucketColumns;
  private final List<TypedField> columns;
  private final CompilationCache cache = new CompilationCache();
  private final Map<Measure, CompiledMeasure> subQueryMeasures;
  private final Map<Measure, CompiledMeasure> measures;
  private final Map<ColumnSetKey, CompiledColumnSet> compiledColumnSets;

  public QueryResolver(QueryDto query, Map<String, Store> storesByName) {
    this.query = query;
    this.storesByName = storesByName;
    if (query.virtualTableDto != null) {
      this.storesByName.put(query.virtualTableDto.name, VirtualTableDto.toStore(query.virtualTableDto));
    }
    this.columns = query.columns.stream().map(this::resolveField).toList();
    this.bucketColumns = Optional.ofNullable(query.columnSets.get(ColumnSetKey.BUCKET))
            .stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()).map(this::resolveField).toList();
    this.subQueryMeasures = query.subQuery == null ? Collections.emptyMap() : compileMeasures(query.subQuery.measures, false);
    this.scope = toQueryScope(query);
    this.measures = compileMeasures(query.measures, true);
    this.compiledColumnSets = compiledColumnSets(query.columnSets);
  }

  /**
   * Retrieves or resolves the {@link TypedField} corresponding to the given Field.
   *
   * @param field The {@link Field} to retrieve or resolve.
   * @return The resolved {@link TypedField} corresponding to the given {@link Field}.
   */
  public TypedField getOrResolveTypedField(Field field) {
    return resolveField(field);
  }

  /**
   * Retrieves or returns null the {@link TypedField} corresponding to the given {@link Field}.
   *
   * @param field The {@link Field} to retrieve.
   * @return The resolved {@link TypedField} corresponding to the given {@link Field}, or null if it is not found.
   */
  public TypedField getTypedFieldOrNull(Field field) {
    return this.cache.getCompiledFields().get(field);
  }

  /**
   * Field resolver
   */
  protected TypedField resolveField(final Field field) {
    return this.cache.computeIfAbsent(field, f -> {
      // Special case for the column that is created due to the column set.
      ColumnSet columnSet = this.query.columnSets.get(BUCKET);
      if (columnSet != null) {
        Field newField = ((BucketColumnSetDto) columnSet).newField;
        if (field.equals(newField)) {
          return new TableTypedField(null, newField.name(), String.class, null);
        }
      }

      if (f instanceof TableField tf) {
        return getTableTypedField(tf.name(), field.alias());
      } else if (f instanceof FunctionField ff) {
        return new FunctionTypedField(getTableTypedField(ff.field.name(), ff.field.alias()), ff.function, ff.alias);
      } else if (f instanceof BinaryOperationField ff) {
        return new BinaryOperationTypedField(ff.operator, resolveField(ff.leftOperand), resolveField(ff.rightOperand), ff.alias);
      } else if (f instanceof ConstantField ff) {
        return new ConstantTypedField(ff.value);
      } else if (f instanceof AliasedField af) {
        return new AliasedTypedField(af.alias());
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
      return new TableTypedField(null, field.name(), Number.class, field.alias());
    }
  }

  private TableTypedField getTableTypedField(String fieldName, String alias) {
    final String[] split = fieldName.split("\\.");
    if (split.length > 1) {
      final String tableName = split[0];
      final String fieldNameInTable = split[1];
      Store store = this.storesByName.get(tableName);
      if (store != null) {
        for (TableTypedField field : store.fields()) {
          if (field.name().equals(fieldNameInTable)) {
            return alias == null ? field : new TableTypedField(field.store(), field.name(), field.type(), alias);
          }
        }
      }
    } else {
      for (Store store : this.storesByName.values()) {
        for (TableTypedField field : store.fields()) {
          if (field.name().equals(fieldName)) {
            // We omit on purpose the store name. It will be determined by the underlying SQL engine of the DB.
            // if any ambiguity, the DB will raise an exception.
            return new TableTypedField(null, fieldName, field.type(), alias);
          }
        }
      }
    }

    if (fieldName.equals(CountMeasure.INSTANCE.alias())) {
      return new TableTypedField(null, CountMeasure.INSTANCE.alias(), long.class, alias);
    }
    throw new FieldNotFoundException("Cannot find field with name " + fieldName);
  }

  /**
   * Queries
   */
  private QueryExecutor.QueryScope toQueryScope(final QueryDto query) {
    checkQuery(query);
    final List<TypedField> columnSets = query.columnSets.values().stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()).map(this::resolveField).toList();
    final List<TypedField> combinedColumns = Stream.concat(this.columns.stream(), columnSets.stream()).toList();

    List<TypedField> rollupColumns = query.rollupColumns.stream().map(this::resolveField).toList();
    List<List<TypedField>> groupingSets = query.groupingSets.stream().map(g -> g.stream().map(this::resolveField).toList()).toList();
    return new QueryExecutor.QueryScope(compileTable(query.table),
            query.subQuery == null ? null : toSubQuery(query.subQuery),
            combinedColumns,
            compileCriteria(query.whereCriteriaDto),
            compileCriteria(query.havingCriteriaDto),
            rollupColumns,
            groupingSets,
            query.virtualTableDto,
            query.limit);
  }

  protected void checkQuery(final QueryDto query) {
    if (query.table == null && query.subQuery == null) {
      throw new IllegalArgumentException("A table or sub-query was expected in " + query);
    } else if (query.table != null && query.subQuery != null) {
      throw new IllegalArgumentException("Cannot define a table and a sub-query at the same time in " + query);
    }
  }

  private DatabaseQuery toSubQuery(final QueryDto subQuery) {
    checkSubQuery(subQuery);
    final CompiledTable table = compileTable(subQuery.table);
    final List<TypedField> select = subQuery.columns.stream().map(this::resolveField).toList();
    final CompiledCriteria whereCriteria = compileCriteria(subQuery.whereCriteriaDto);
    final CompiledCriteria havingCriteria = compileCriteria(subQuery.havingCriteriaDto);
    // should we check groupingSet and rollup as well are empty ?
    DatabaseQuery query = new DatabaseQuery(null,
            table,
            null,
            new HashSet<>(select),
            whereCriteria,
            havingCriteria,
            Collections.emptyList(),
            Collections.emptyList(),
            subQuery.limit);
    this.subQueryMeasures.values().forEach(query::withMeasure);
    return query;
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

  public DatabaseQuery toDatabaseQuery(final QueryExecutor.QueryScope query, final int limit) {
    return new DatabaseQuery(query.virtualTable(),
            query.table(),
            query.subQuery(),
            new LinkedHashSet<>(query.columns()),
            query.whereCriteria(),
            query.havingCriteria(),
            query.rollupColumns(),
            query.groupingSets(),
            limit);
  }

  /**
   * Table
   */
  private CompiledTable compileTable(final TableDto table) {
    return table == null
            ? null
            : new CompiledTable(table.name, table.joins.stream().map(this::compileJoin).collect(Collectors.toList()));
  }

  private CompiledTable.CompiledJoin compileJoin(JoinDto join) {
    return new CompiledTable.CompiledJoin(compileTable(join.table), join.type, compileCriteria(join.joinCriteria));
  }

  /**
   * Criteria
   */
  private CompiledCriteria compileCriteria(final CriteriaDto criteria) {
    return criteria == null
            ? null
            : this.cache.computeIfAbsent(criteria, c -> new CompiledCriteria(c.condition, c.conditionType, c.field == null ? null : resolveWithFallback(c.field), c.fieldOther == null ? null : resolveWithFallback(c.fieldOther),
            c.measure == null ? null : compileMeasure(c.measure, true),
            c.children.stream().map(this::compileCriteria).collect(Collectors.toList())));
  }

  /**
   * Compiles measures
   */
  private Map<Measure, CompiledMeasure> compileMeasures(final List<Measure> measures, boolean topMeasures) {
    return measures
            .stream()
            .collect(Collectors.toMap(Function.identity(), m -> compileMeasure(m, topMeasures)));
  }

  protected CompiledMeasure compileMeasure(Measure measure, boolean topMeasure) {
    return this.cache.computeIfAbsent(measure, m -> {
      final CompiledMeasure compiledMeasure;
      if (m instanceof AggregatedMeasure) {
        compiledMeasure = compileAggregatedMeasure((AggregatedMeasure) m);
      } else if (m instanceof ExpressionMeasure) {
        compiledMeasure = compileExpressionMeasure((ExpressionMeasure) m);
      } else if (m instanceof ConstantMeasure<?>) {
        compiledMeasure = compileConstantMeasure((ConstantMeasure<?>) m);
      } else if (m instanceof BinaryOperationMeasure) {
        compiledMeasure = compileBinaryOperationMeasure((BinaryOperationMeasure) m, topMeasure);
      } else if (m instanceof ComparisonMeasureReferencePosition) {
        compiledMeasure = compileComparisonMeasure((ComparisonMeasureReferencePosition) m, topMeasure);
      } else if (m instanceof VectorAggMeasure v) {
        compiledMeasure = compileVectorAggMeasure(v);
      } else if (m instanceof VectorTupleAggMeasure v) {
        compiledMeasure = compileVectorTupleAggMeasure(v);
      } else {
        throw new IllegalArgumentException("Unknown type of measure " + m.getClass().getSimpleName());
      }
      if (topMeasure) {
        return compiledMeasure;
      } else if (MeasureUtils.isPrimitive(compiledMeasure)) {
        return compiledMeasure;
      }
      throw new IllegalArgumentException("Only measures that can be computed by the underlying database can be used" +
              " in a sub-query but " + m + " was provided");
    });
  }

  private CompiledMeasure compileAggregatedMeasure(AggregatedMeasure m) {
    if (m.equals(CountMeasure.INSTANCE)) {
      return COMPILED_COUNT;
    }
    return new CompiledAggregatedMeasure(m.alias, resolveWithFallback(m.field), m.aggregationFunction, compileCriteria(m.criteria), m.distinct);
  }

  private CompiledMeasure compileExpressionMeasure(ExpressionMeasure m) {
    return new CompiledExpressionMeasure(m.alias, m.expression);
  }

  private CompiledMeasure compileConstantMeasure(ConstantMeasure<?> m) {
    if (m instanceof DoubleConstantMeasure dc) {
      return new CompiledDoubleConstantMeasure(dc.value);
    } else if (m instanceof LongConstantMeasure lc) {
      return new CompiledLongConstantMeasure(lc.value);
    } else {
      throw new IllegalArgumentException("unexpected type " + m.getClass());
    }
  }

  private CompiledMeasure compileBinaryOperationMeasure(BinaryOperationMeasure m, boolean topMeasure) {
    return new CompiledBinaryOperationMeasure(m.alias, m.operator, compileMeasure(m.leftOperand, topMeasure), compileMeasure(m.rightOperand, topMeasure));
  }

  private CompiledMeasure compileComparisonMeasure(ComparisonMeasureReferencePosition m, boolean topMeasure) {
    return new CompiledComparisonMeasure(
            m.alias,
            m.comparisonMethod,
            m.comparisonOperator,
            compileMeasure(m.measure, topMeasure),
            m.referencePosition == null ? null : m.referencePosition.entrySet().stream().collect(Collectors.toMap(e -> resolveField(e.getKey()), Map.Entry::getValue)),
            compilePeriod(m.period),
            m.columnSetKey,
            m.ancestors == null ? null : m.ancestors.stream().map(this::resolveField).collect(Collectors.toList()));
  }

  private CompiledPeriod compilePeriod(Period period) {
    if (period == null) {
      return null;
    }
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

  private CompiledMeasure compileVectorAggMeasure(VectorAggMeasure m) {
    return new CompiledVectorAggMeasure(m.alias, resolveField(m.fieldToAggregate), m.aggregationFunction, resolveField(m.vectorAxis));
  }

  private CompiledMeasure compileVectorTupleAggMeasure(VectorTupleAggMeasure m) {
    return new CompiledVectorTupleAggMeasure(
            m.alias,
            m.fieldToAggregateAndAggFunc.stream().map(p -> new CompiledFieldAndAggFunc(resolveField(p.field), p.aggFunc)).toList(),
            resolveField(m.vectorAxis),
            m.transformer);
  }

  private Map<ColumnSetKey, CompiledColumnSet> compiledColumnSets(Map<ColumnSetKey, ColumnSet> columnSets) {
    Map<ColumnSetKey, CompiledColumnSet> m = new HashMap<>();
    for (Map.Entry<ColumnSetKey, ColumnSet> entry : columnSets.entrySet()) {
      if (entry.getKey() != BUCKET) {
        throw new IllegalArgumentException("unexpected column set " + entry.getValue());
      }
      BucketColumnSetDto bucket = (BucketColumnSetDto) entry.getValue();
      m.put(entry.getKey(), new CompiledBucketColumnSet(
              bucket.getColumnsForPrefetching().stream().map(this::resolveField).toList(),
              bucket.getNewColumns().stream().map(this::resolveField).toList(),
              entry.getKey(),
              bucket.values));
    }
    return m;
  }

  @Value
  private static class CompilationCache {
    Map<Field, TypedField> compiledFields = new ConcurrentHashMap<>();
    Map<Measure, CompiledMeasure> compiledMeasure = new ConcurrentHashMap<>();
    Map<CriteriaDto, CompiledCriteria> compiledCriteria = new ConcurrentHashMap<>();

    /**
     * We don't rely on Map.computeIfAbsent directly as it doesn't allow recursive updates
     */
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
