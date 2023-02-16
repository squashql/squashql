package io.squashql.query;

import io.squashql.query.comp.BinaryOperations;
import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.store.Field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.squashql.query.ColumnSetKey.BUCKET;

public class Evaluator implements BiConsumer<QueryExecutor.QueryPlanNodeKey, QueryExecutor.ExecutionContext>, MeasureVisitor<Void> {

  private final Function<String, Field> fieldSupplier;
  private QueryExecutor.ExecutionContext executionContext;

  public Evaluator(Function<String, Field> fieldSupplier) {
    this.fieldSupplier = fieldSupplier;
  }

  @Override
  public void accept(QueryExecutor.QueryPlanNodeKey queryPlanNodeKey, QueryExecutor.ExecutionContext executionContext) {
    Measure measure = queryPlanNodeKey.measure();
    if (executionContext.writeToTable().measures().contains(measure)) {
      return; // nothing to do
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
    Field field = new Field(bom.alias(), BinaryOperations.getOutputType(bom.operator, lType, rType));
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
    executeComparator(cm, this.executionContext.writeToTable(), readFromTable, executor);
    return null;
  }

  private static void executeComparator(ComparisonMeasureReferencePosition cm, Table writeToTable, Table readFromTable, AComparisonExecutor executor) {
    List<Object> agg = executor.compare(cm, writeToTable, readFromTable);
    Field field = new Field(cm.alias(), BinaryOperations.getComparisonOutputType(cm.comparisonMethod, writeToTable.getField(cm.measure).type()));
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
    Field field = new Field(cm.alias(), type);
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
