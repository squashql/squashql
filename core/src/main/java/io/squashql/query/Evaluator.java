package io.squashql.query;

import io.squashql.query.QueryExecutor.ExecutionContext;
import io.squashql.query.QueryExecutor.QueryPlanNodeKey;
import io.squashql.query.comp.BinaryOperations;
import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.store.FieldWithStore;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.squashql.query.ColumnSetKey.BUCKET;

public class Evaluator implements BiConsumer<QueryPlanNodeKey, ExecutionContext>, MeasureVisitor<Void> {

  private final Function<String, FieldWithStore> fieldSupplier;
  private ExecutionContext executionContext;

  public Evaluator(Function<String, FieldWithStore> fieldSupplier) {
    this.fieldSupplier = fieldSupplier;
  }

  @Override
  public void accept(QueryPlanNodeKey queryPlanNodeKey, ExecutionContext executionContext) {
    if (!queryPlanNodeKey.queryScope().equals(executionContext.queryScope())) {
      // Not in the correct plan, abort execution. This condition is very important to not write aggregates where they
      // should not suppose to be written. The whole dependency graph (See QueryExecutor) can be traversed multiple times
      // when there are more than 1 execution plan.
      return;
    }

    Measure measure = queryPlanNodeKey.measure();
    if (executionContext.writeToTable().measures().contains(measure)) {
      return; // Nothing to do
    }

    this.executionContext = executionContext;
    executionContext.queryWatch().start(queryPlanNodeKey);
    measure.accept(this);
    executionContext.queryWatch().stop(queryPlanNodeKey);
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
    FieldWithStore field = new FieldWithStore(null, bom.alias(), BinaryOperations.getOutputType(bom.operator, lType, rType));
    intermediateResult.addAggregates(field, bom, r);
    return null;
  }

  @Override
  public Void visit(ComparisonMeasureReferencePosition cm) {
    AComparisonExecutor executor;
    if (cm.columnSetKey == BUCKET) {
      ColumnSet cs = this.executionContext.query().columnSets.get(cm.columnSetKey);
      if (cs == null) {
        throw new IllegalArgumentException(String.format("columnSet %s is not specified in the query but is used in a comparison measure: %s", cm.columnSetKey, cm));
      }
      executor = new BucketComparisonExecutor((BucketColumnSetDto) cs);
    } else if (cm.period != null) {
      for (String field : cm.period.getFields()) {
        if (!this.executionContext.query().columns.contains(field)) {
          throw new IllegalArgumentException(String.format("%s is not specified in the query but is used in a comparison measure: %s", field, cm));
        }
      }
      executor = new PeriodComparisonExecutor(cm);
    } else if (cm.ancestors != null) {
      executor = new ParentComparisonExecutor(cm);
    } else {
      throw new IllegalArgumentException(String.format("Comparison measure not correctly defined (%s). It should have a period or columnSetKey parameter", cm));
    }

    QueryExecutor.QueryScope readScope = MeasureUtils.getReadScopeComparisonMeasureReferencePosition(
            this.executionContext.query(), cm, this.executionContext.queryScope(), this.fieldSupplier);
    Table readFromTable = this.executionContext.tableByScope().get(readScope); // Table where to read the aggregates
    if (readFromTable.count() == this.executionContext.query().limit) {
      throw new RuntimeException("Too many rows, some intermediate results exceed the limit " + this.executionContext.query().limit);
    }
    executeComparator(cm, this.executionContext.writeToTable(), readFromTable, executor);
    return null;
  }

  private static void executeComparator(ComparisonMeasureReferencePosition cm, Table writeToTable, Table readFromTable, AComparisonExecutor executor) {
    List<Object> agg = executor.compare(cm, writeToTable, readFromTable);
    FieldWithStore field = new FieldWithStore(null, cm.alias(), BinaryOperations.getComparisonOutputType(cm.comparisonMethod, writeToTable.getField(cm.measure).type()));
    writeToTable.addAggregates(field, cm, agg);
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
    FieldWithStore field = new FieldWithStore(null, cm.alias(), type);
    List<Object> r = Collections.nCopies((int) intermediateResult.count(), v);
    intermediateResult.addAggregates(field, cm, r);
  }

  // The following measures are not evaluated here but in the underlying DB.

  @Override
  public Void visit(AggregatedMeasure measure) {
    throw new IllegalStateException(AggregatedMeasure.class.getName());
  }

  @Override
  public Void visit(ExpressionMeasure measure) {
    throw new IllegalStateException(ExpressionMeasure.class.getSimpleName());
  }
}
