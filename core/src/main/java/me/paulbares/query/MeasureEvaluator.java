package me.paulbares.query;

import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.dto.BucketColumnSetDto;
import me.paulbares.query.dto.PeriodColumnSetDto;
import me.paulbares.store.Field;
import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.MutableIntIntMapFactoryImpl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static me.paulbares.query.ColumnSetKey.BUCKET;
import static me.paulbares.query.ColumnSetKey.PERIOD;

public class MeasureEvaluator implements BiConsumer<QueryExecutor.QueryPlanNodeKey, QueryExecutor.ExecutionContext>, MeasureVisitor<Void> {

  private final Function<String, Field> fieldSupplier;
  private QueryExecutor.ExecutionContext executionContext;

  public MeasureEvaluator(Function<String, Field> fieldSupplier) {
    this.fieldSupplier = fieldSupplier;
  }

  @Override
  public void accept(QueryExecutor.QueryPlanNodeKey queryPlanNodeKey, QueryExecutor.ExecutionContext executionContext) {
    Measure measure = queryPlanNodeKey.measure();
    if (executionContext.writeToTable().measures().contains(measure)) {
      return; // nothing to do
    }
    this.executionContext = executionContext;
    executionContext.queryWatch().start(measure);
    measure.accept(this);
    executionContext.queryWatch().stop(measure);
  }

  @Override
  public Void visit(BinaryOperationMeasure bom) {
    Table intermediateResult = this.executionContext.writeToTable();
    List<Object> lo = intermediateResult.getAggregateValues(bom.leftOperand);
    List<Object> ro = intermediateResult.getAggregateValues(bom.rightOperand);
    List<Object> r = new ArrayList<>(lo.size());

    Class<?> lType = intermediateResult.getField(bom.leftOperand).type();
    Class<?> rType = intermediateResult.getField(bom.rightOperand).type();
    BiFunction<Number, Number, Number> operation = BinaryOperations.createBiFunction(bom.operator, lType, rType);
    for (int i = 0; i < lo.size(); i++) {
      r.add(operation.apply((Number) lo.get(i), (Number) ro.get(i)));
    }
    Field field = new Field(bom.alias(), BinaryOperations.getOutputType(bom.operator, lType, rType));
    intermediateResult.addAggregates(field, bom, r);
    return null;
  }

  @Override
  public Void visit(ComparisonMeasureReferencePosition cm) {
    Map<ColumnSetKey, Function<ColumnSet, AComparisonExecutor>> m = Map.of(
            BUCKET, cs -> new BucketComparisonExecutor((BucketColumnSetDto) cs),
            PERIOD, cs -> new PeriodComparisonExecutor((PeriodColumnSetDto) cs));
    ColumnSet cs = this.executionContext.query().columnSets.get(cm.columnSetKey);
    if (cs == null) {
      throw new IllegalArgumentException(String.format("columnSet %s is not specified in the query but is used in a comparison measure: %s", cm.columnSetKey, cm));
    }
    AComparisonExecutor executor = m.get(cm.columnSetKey).apply(cs);
    if (executor != null) {
      QueryExecutor.QueryScope readScope = MeasureUtils.getReadScopeComparisonMeasureReferencePosition(this.executionContext.query(), this.executionContext.queryScope());
      Table readFromTable = this.executionContext.tableByScope().get(readScope); // Table where to read the aggregates
      executeComparator(cm, this.executionContext.writeToTable(), readFromTable, executor);
    }
    return null;
  }

  private static void executeComparator(ComparisonMeasureReferencePosition cm, Table writeToTable, Table readFromTable, AComparisonExecutor executor) {
    List<Object> agg = executor.compare(cm, writeToTable, readFromTable);
    Field field = new Field(cm.alias(), BinaryOperations.getComparisonOutputType(cm.comparisonMethod, writeToTable.getField(cm.measure).type()));
    writeToTable.addAggregates(field, cm, agg);
  }

  @Override
  public Void visit(ParentComparisonMeasure pcm) {
    Table whereToWrite = this.executionContext.writeToTable();
    QueryExecutor.QueryScope parentScope = MeasureUtils.getParentScopeWithClearedConditions(this.executionContext.queryScope(), pcm, this.fieldSupplier);
    Table whereToRead = this.executionContext.tableByScope().get(parentScope);
    List<Object> aggregateValues = whereToWrite.getAggregateValues(pcm.measure);
    List<Object> parentAggregateValues = whereToRead.getAggregateValues(pcm.measure);
    List<Object> result = new ArrayList<>((int) whereToWrite.count());
    BiFunction<Number, Number, Number> divide = BinaryOperations.createComparisonBiFunction(ComparisonMethod.DIVIDE, double.class);

    MutableIntIntMap mapping = MutableIntIntMapFactoryImpl.INSTANCE.empty();
    for (int index : whereToRead.columnIndices()) {
      Field field = whereToRead.headers().get(index);
      int indexWhereToWrite = whereToWrite.index(field);
      mapping.put(index, indexWhereToWrite);
    }

    int[] rowIndex = new int[1];
    whereToWrite.forEach(row -> {
      // Start - Shift operation
      Object[] parentRow = new Object[parentScope.columns().size()];
      for (int columnIndex : whereToRead.columnIndices()) {
        int index = mapping.getIfAbsent(columnIndex, -1);
        parentRow[columnIndex] = row.get(index);
      }
      // End - Shift operation

      int position = whereToRead.pointDictionary().getPosition(parentRow);
      if (position != -1) {
        Object referenceValue = parentAggregateValues.get(position);
        Object currentValue = aggregateValues.get(rowIndex[0]);
        Object div = divide.apply((Number) currentValue, (Number) referenceValue);
        result.add(div);
      } else {
        result.add(null); // nothing to compare with
      }
      rowIndex[0]++;
    });

    Field field = new Field(pcm.alias(), double.class);
    whereToWrite.addAggregates(field, pcm, result);
    return null;
  }

  @Override
  public Void visit(LongConstantMeasure measure) {
    executeConstantOperation(measure, this.executionContext.writeToTable());
    return null;
  }

  @Override
  public Void visit(DoubleConstantMeasure measure) {
    executeConstantOperation(measure, this.executionContext.writeToTable());
    return null;
  }

  private static void executeConstantOperation(ConstantMeasure<?> cm, Table intermediateResult) {
    Object v;
    Class<?> type;
    if (cm instanceof DoubleConstantMeasure dcm) {
      v = ((Number) dcm.value).doubleValue();
      type = double.class;
    } else if (cm instanceof LongConstantMeasure lcm) {
      v = ((Number) lcm.value).longValue();
      type = long.class;
    } else {
      throw new IllegalArgumentException("Unexpected type " + cm.getValue().getClass() + ". Only double and long are supported");
    }
    Field field = new Field(cm.alias(), type);
    List<Object> r = Collections.nCopies((int) intermediateResult.count(), v);
    intermediateResult.addAggregates(field, cm, r);
  }

  // The following measures are not evaluated here but in the underlying DB.

  @Override
  public Void visit(UnresolvedExpressionMeasure measure) {
    throw new IllegalStateException();
  }

  @Override
  public Void visit(AggregatedMeasure measure) {
    throw new IllegalStateException();
  }

  @Override
  public Void visit(ExpressionMeasure measure) {
    throw new IllegalStateException();
  }
}
