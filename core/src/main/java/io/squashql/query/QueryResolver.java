package io.squashql.query;

import io.squashql.query.compiled.*;
import io.squashql.query.database.DatabaseQuery;
import io.squashql.query.database.QueryScope;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.*;
import io.squashql.query.exception.FieldNotFoundException;
import io.squashql.query.measure.ParametrizedMeasure;
import io.squashql.query.measure.Repository;
import io.squashql.store.Store;
import io.squashql.type.*;
import lombok.Data;
import lombok.Value;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.squashql.query.ColumnSetKey.GROUP;
import static io.squashql.query.compiled.CompiledAggregatedMeasure.COMPILED_COUNT;
import static io.squashql.query.dto.SingleValueConditionDto.SUPPORTED_TYPES;

@Data
public class QueryResolver {

  private final QueryDto query;
  private final Map<String, Store> storeByName;
  private final Set<String> cteTableNames = new HashSet<>();
  private final QueryScope scope;
  private final List<TypedField> groupColumns;
  private final List<TypedField> columns;
  private final CompilationCache cache = new CompilationCache();
  private final Map<Measure, CompiledMeasure> measures;
  private final Map<ColumnSetKey, CompiledColumnSet> compiledColumnSets;
  /**
   * The {@link OrderDto} to be computed only by the DB. Excluding orders made on measures computed by SquashQL.
   */
  private final List<CompiledOrderBy> compiledOrderByInDB;

  public QueryResolver(QueryDto query, Map<String, Store> storeByName) {
    this.query = query;
    this.storeByName = createStoreByName(query, storeByName);
    this.columns = query.columns.stream().map(this::resolveField).toList();
    this.groupColumns = Optional.ofNullable(query.columnSets.get(ColumnSetKey.GROUP))
            .stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()).map(this::resolveField).toList();
    this.compiledOrderByInDB = compileOrderByInDB(query.orders);
    this.scope = toQueryScope(query);
    this.measures = compileMeasures(query.measures, true);
    this.compiledColumnSets = compiledColumnSets(query.columnSets);
  }

  private Map<String, Store> createStoreByName(QueryDto query, Map<String, Store> storeByName) {
    Map<String, Store> res = new HashMap<>(storeByName);
    if (query.virtualTableDtos != null) {
      for (VirtualTableDto virtualTableDto : query.virtualTableDtos) {
        res.put(virtualTableDto.name, VirtualTableDto.toStore(virtualTableDto));
        this.cteTableNames.add(virtualTableDto.name);
      }
    }
    return res;
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
  public TypedField resolveField(Field field) {
    return this.cache.computeIfAbsent(field, f -> {
      // Special case for the column that is created due to the column set.
      ColumnSet columnSet = this.query.columnSets.get(GROUP);
      if (columnSet != null) {
        Field newField = ((GroupColumnSetDto) columnSet).newField;
        if (field.equals(newField)) {
          return new TableTypedField(null, ((TableField) newField).fullName, String.class, null, false);
        }
      }

      if (f instanceof TableField tf) {
        return getTableTypedField(tf.fullName, field.alias());
      } else if (f instanceof FunctionField ff) {
        return new FunctionTypedField(ff.operand != null ? resolveField(ff.operand) : null, ff.functionName, ff.alias);
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
      String name = null;
      if (field instanceof AliasedField af) {
        name = af.alias;
      } else if (field instanceof TableField tf) {
        name = tf.fullName;
      }

      if (name != null) {
        return new TableTypedField(null, name, Number.class, field.alias(), false);
      }

      throw e;
    }
  }

  private TableTypedField getTableTypedField(String fieldName, String alias) {
    String[] split = fieldName.split("\\.");
    if (split.length > 1) {
      String tableName = split[0];
      String fieldNameInTable = split[1];
      Store store = this.storeByName.get(tableName);
      if (store != null) {
        for (TableTypedField field : store.fields()) {
          if (field.name().equals(fieldNameInTable)) {
            return alias == null ? field : new TableTypedField(field.store(), field.name(), field.type(), alias, this.cteTableNames.contains(store.name()));
          }
        }
      }
    } else {
      for (Store store : this.storeByName.values()) {
        for (TableTypedField field : store.fields()) {
          if (field.name().equals(fieldName)) {
            // We omit on purpose the store name. It will be determined by the underlying SQL engine of the DB.
            // if any ambiguity, the DB will raise an exception.
            return new TableTypedField(null, fieldName, field.type(), alias, this.cteTableNames.contains(store.name()));
          }
        }
      }
    }

    if (fieldName.equals(CountMeasure.INSTANCE.alias())) {
      return new TableTypedField(null, CountMeasure.INSTANCE.alias(), long.class, alias, false);
    }
    throw new FieldNotFoundException("Cannot find field with name " + fieldName);
  }

  private QueryScope toQueryScope(QueryDto query) {
    checkQuery(query);
    List<TypedField> columnSets = query.columnSets.values().stream().flatMap(cs -> cs.getColumnsForPrefetching().stream()).map(this::resolveField).toList();
    List<TypedField> combinedColumns = Stream.concat(this.columns.stream(), columnSets.stream()).toList();

    List<TypedField> rollup = query.rollupColumns.stream().map(this::resolveField).toList();
    Set<Set<TypedField>> groupingSets = query.groupingSets.stream().map(g -> g.stream().map(this::resolveField).collect(Collectors.toSet())).collect(Collectors.toSet());
    return new QueryScope(
            compileTable(query.table),
            combinedColumns,
            compileCriteria(query.whereCriteriaDto),
            compileCriteria(query.havingCriteriaDto),
            rollup,
            groupingSets,
            compileVirtualTables(query.virtualTableDtos),
            this.compiledOrderByInDB,
            query.limit);
  }

  protected void checkQuery(QueryDto query) {
    if (query.table.name == null && query.table.subQuery == null) {
      throw new IllegalArgumentException("A table or sub-query was expected in " + query);
    } else if (query.table.name != null && query.table.subQuery != null) {
      throw new IllegalArgumentException("Cannot define a table and a sub-query at the same time in " + query);
    }
  }

  private void checkSubQuery(QueryDto subQuery) {
    compileMeasures(subQuery.measures, false);
    if (subQuery.columnSets != null && !subQuery.columnSets.isEmpty()) {
      throw new IllegalArgumentException("column sets are not expected in sub query: " + subQuery);
    }
    if (subQuery.parameters != null && !subQuery.parameters.isEmpty()) {
      throw new IllegalArgumentException("parameters are not expected in sub query: " + subQuery);
    }
  }

  /**
   * Table
   */
  public CompiledTable compileTable(TableDto table) {
    if (table.name != null) {
      List<CompiledJoin> joins = compileJoins(table.joins);
      if (table.isCte) {
        return new CteTable(table.name, joins);
      } else {
        // Can be a cte.
        List<VirtualTableDto> vts = this.query.virtualTableDtos;
        if (vts != null) {
          List<CteRecordTable> cteRecordTables = compileVirtualTables(vts);
          for (CteRecordTable cteRecordTable : cteRecordTables) {
            if (cteRecordTable.name().equals(table.name)) {
              return cteRecordTable;
            }
          }
        }
      }
      return new MaterializedTable(table.name, joins);
    } else if (table.subQuery != null) {
      List<CompiledJoin> joins = compileJoins(table.joins);
      checkSubQuery(table.subQuery);
      QueryResolver qr = new QueryResolver(table.subQuery, this.storeByName);
      DatabaseQuery dq = new DatabaseQuery(qr.getScope(), new ArrayList<>(qr.measures.values()));
      return new NestedQueryTable(dq, joins);
    } else {
      throw new IllegalStateException();
    }
  }

  private List<CteRecordTable> compileVirtualTables(List<VirtualTableDto> virtualTableDtos) {
    return virtualTableDtos.stream().map(v -> new CteRecordTable(v.name, v.fields, v.records)).toList();
  }

  /**
   * Joins
   */
  public List<CompiledJoin> compileJoins(List<JoinDto> joins) {
    return joins.stream().map(this::compileJoin).collect(Collectors.toList());
  }

  public CompiledJoin compileJoin(JoinDto join) {
    CompiledTable table = compileTable(join.table);
    if (table instanceof NamedTable nt) {
      return new CompiledJoin(nt, join.type, compileCriteria(join.joinCriteria));
    } else {
      throw new IllegalArgumentException("expected table of type " + NamedTable.class + " but received table of type " + table.getClass());
    }
  }

  /**
   * Criteria
   */
  public CompiledCriteria compileCriteria(CriteriaDto criteria) {
    return criteria == null
            ? null
            : this.cache.computeIfAbsent(criteria, c -> {
      if (c.field != null && c.condition instanceof SingleValueConditionDto svc && svc.value instanceof Field other) {
        // transform the condition
        return compileCriteria(new CriteriaDto(c.field, other, c.condition.type()));
      } else if (c.field != null && c.fieldOther != null && c.conditionType != null && SUPPORTED_TYPES.contains(c.conditionType)) {
        // transform the condition
        if (c.field instanceof ConstantField cf) {
          return compileCriteria(new CriteriaDto(c.fieldOther, new SingleValueConditionDto(c.conditionType, cf.value)));
        } else if (c.fieldOther instanceof ConstantField cf) {
          return compileCriteria(new CriteriaDto(c.field, new SingleValueConditionDto(c.conditionType, cf.value)));
        }
      }
      return new CompiledCriteria(
              c.condition,
              c.conditionType,
              c.field == null ? null : resolveWithFallback(c.field),
              c.fieldOther == null ? null : resolveWithFallback(c.fieldOther),
              c.measure == null ? null : compileMeasure(c.measure, true),
              c.children.stream().map(this::compileCriteria).collect(Collectors.toList()));
    });
  }

  /**
   * Compiles measures
   */
  private Map<Measure, CompiledMeasure> compileMeasures(List<Measure> measures, boolean topMeasures) {
    return measures
            .stream()
            .collect(Collectors.toMap(Function.identity(), m -> compileMeasure(m, topMeasures)));
  }

  protected CompiledMeasure compileMeasure(Measure measure, boolean topMeasure) {
    return this.cache.computeIfAbsent(measure, m -> {
      CompiledMeasure compiledMeasure;
      if (m instanceof AggregatedMeasure am) {
        compiledMeasure = compileAggregatedMeasure(am);
      } else if (m instanceof ExpressionMeasure em) {
        compiledMeasure = compileExpressionMeasure(em);
      } else if (m instanceof ConstantMeasure<?> cm) {
        compiledMeasure = compileConstantMeasure(cm);
      } else if (m instanceof BinaryOperationMeasure bom) {
        compiledMeasure = compileBinaryOperationMeasure(bom, topMeasure);
      } else if (m instanceof ComparisonMeasureReferencePosition cmrp) {
        compiledMeasure = compileComparisonMeasure(cmrp, topMeasure);
      } else if (m instanceof ComparisonMeasureGrandTotal cmgt) {
        compiledMeasure = new CompiledGrandTotalComparisonMeasure(cmgt.alias, cmgt.comparisonMethod, compileMeasure(cmgt.measure, topMeasure));
      } else if (m instanceof VectorAggMeasure v) {
        compiledMeasure = compileVectorAggMeasure(v);
      } else if (m instanceof VectorTupleAggMeasure v) {
        compiledMeasure = compileVectorTupleAggMeasure(v);
      } else if (m instanceof ParametrizedMeasure pm) {
        compiledMeasure = compileMeasure(Repository.create(pm), topMeasure);
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
    return new CompiledComparisonMeasureReferencePosition(
            m.alias,
            m.comparisonMethod,
            m.comparisonOperator,
            m.clearFilters,
            compileMeasure(m.measure, topMeasure),
            m.referencePosition == null ? null : m.referencePosition.entrySet().stream().collect(Collectors.toMap(e -> resolveField(e.getKey()), Map.Entry::getValue)),
            compilePeriod(m.period),
            m.columnSetKey,
            m.elements,
            m.ancestors == null ? null : m.ancestors.stream().map(this::resolveField).collect(Collectors.toList()),
            m.grandTotalAlongAncestors);
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
      if (entry.getKey() != GROUP) {
        throw new IllegalArgumentException("unexpected column set " + entry.getValue());
      }
      GroupColumnSetDto group = (GroupColumnSetDto) entry.getValue();
      m.put(entry.getKey(), new CompiledGroupColumnSet(
              group.getColumnsForPrefetching().stream().map(this::resolveField).toList(),
              group.getNewColumns().stream().map(this::resolveField).toList(),
              entry.getKey(),
              group.values));
    }
    return m;
  }

  /**
   * Compiles orderBy but remove the ones concerning measures computed by SquashQL.
   */
  private List<CompiledOrderBy> compileOrderByInDB(Map<Field, OrderDto> orders) {
    List<CompiledOrderBy> r = new ArrayList<>();
    for (Map.Entry<Field, OrderDto> e : orders.entrySet()) {
      String expression = SqlUtils.squashqlExpression(e.getKey());
      Optional<Measure> first = this.query.measures.stream().filter(m -> m.alias().equals(expression)).findFirst();
      if (first.isPresent() && !MeasureUtils.isPrimitive(compileMeasure(first.get(), true))) {
        continue;
      }
      TypedField key = resolveWithFallback(e.getKey());
      r.add(new CompiledOrderBy(key, e.getValue()));
    }
    return r;
  }

  @Value
  private static class CompilationCache {

    Map<Field, TypedField> compiledFields = new ConcurrentHashMap<>();
    Map<Measure, CompiledMeasure> compiledMeasure = new ConcurrentHashMap<>();
    Map<CriteriaDto, CompiledCriteria> compiledCriteria = new ConcurrentHashMap<>();

    /**
     * We don't rely on Map.computeIfAbsent directly as it doesn't allow recursive updates
     */
    private TypedField computeIfAbsent(Field field, Function<Field, TypedField> mappingFunction) {
      if (this.compiledFields.containsKey(field)) {
        return this.compiledFields.get(field);
      } else {
        TypedField typedField = mappingFunction.apply(field);
        this.compiledFields.put(field, typedField);
        return typedField;
      }
    }

    private CompiledMeasure computeIfAbsent(Measure measure, Function<Measure, CompiledMeasure> mappingFunction) {
      if (this.compiledMeasure.containsKey(measure)) {
        return this.compiledMeasure.get(measure);
      } else {
        CompiledMeasure compiledMeasure = mappingFunction.apply(measure);
        this.compiledMeasure.put(measure, compiledMeasure);
        return compiledMeasure;
      }
    }

    private CompiledCriteria computeIfAbsent(CriteriaDto criteria, Function<CriteriaDto, CompiledCriteria> mappingFunction) {
      if (this.compiledCriteria.containsKey(criteria)) {
        return this.compiledCriteria.get(criteria);
      } else {
        CompiledCriteria compiledCriteria = mappingFunction.apply(criteria);
        this.compiledCriteria.put(criteria, compiledCriteria);
        return compiledCriteria;
      }
    }
  }
}
