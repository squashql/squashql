package io.squashql.query;

import io.squashql.query.QueryExecutor.ExecutionContext;
import io.squashql.query.QueryExecutor.QueryPlanNodeKey;
import io.squashql.query.comp.BinaryOperations;
import io.squashql.query.dto.BucketColumnSetDto;
import io.squashql.table.Table;
import io.squashql.type.TypedField;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.squashql.query.ColumnSetKey.BUCKET;

public class Evaluator implements BiConsumer<QueryPlanNodeKey, ExecutionContext>, MeasureVisitor<Void> {

  private final Function<Field, TypedField> fieldSupplier;
  private ExecutionContext executionContext;

  public Evaluator(Function<Field, TypedField> fieldSupplier) {
    this.fieldSupplier = fieldSupplier;
  }

  @Override
  public void accept(QueryPlanNodeKey queryPlanNodeKey, ExecutionContext executionContext) {
    Measure measure = queryPlanNodeKey.measure();
    if (executionContext.getWriteToTable().measures().contains(measure)) {
      return; // Nothing to do
    }
    this.executionContext = executionContext;
    measure.accept(this);
  }

  @Override
  public Void visit(BinaryOperationMeasure bom) {
    Table intermediateResult = this.executionContext.getWriteToTable();
    List<Object> lo = intermediateResult.getAggregateValues(bom.leftOperand);
    List<Object> ro = intermediateResult.getAggregateValues(bom.rightOperand);
    List<Object> r = new ArrayList<>(lo.size());

    Class<?> lType = intermediateResult.getHeader(bom.leftOperand).type();
    Class<?> rType = intermediateResult.getHeader(bom.rightOperand).type();
    BiFunction<Number, Number, Number> operation = BinaryOperations.createBiFunction(bom.operator, lType, rType);
    for (int i = 0; i < lo.size(); i++) {
      r.add(operation.apply((Number) lo.get(i), (Number) ro.get(i)));
    }
    Header header = new Header(bom.alias, BinaryOperations.getOutputType(bom.operator, lType, rType), true);
    intermediateResult.addAggregates(header, bom, r);
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
      for (Field field : cm.period.getFields()) {
        if (!this.executionContext.query().columns.contains(field)) {
          throw new IllegalArgumentException(String.format("%s is not specified in the query but is used in a comparison measure: %s", field.name(), cm));
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
    if (readFromTable.count() == this.executionContext.queryLimit()) {
      throw new RuntimeException("Too many rows, some intermediate results exceed the limit " + this.executionContext.queryLimit());
    }
    executeComparator(cm, this.executionContext.getWriteToTable(), readFromTable, executor);
    return null;
  }

  private static void executeComparator(ComparisonMeasureReferencePosition cm, Table writeToTable, Table readFromTable, AComparisonExecutor executor) {
    List<Object> agg = executor.compare(cm, writeToTable, readFromTable);
    Header header = new Header(cm.alias(), BinaryOperations.getComparisonOutputType(cm.comparisonMethod, writeToTable.getHeader(cm.measure).type()), true);
    writeToTable.addAggregates(header, cm, agg);
  }

  @Override
  public Void visit(LongConstantMeasure measure) {
    executeConstantOperation(measure, this.executionContext.getWriteToTable());
    return null;
  }

  @Override
  public Void visit(DoubleConstantMeasure measure) {
    executeConstantOperation(measure, this.executionContext.getWriteToTable());
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
    Header header = new Header(cm.alias(), type, true);
    List<Object> r = Collections.nCopies((int) intermediateResult.count(), v);
    intermediateResult.addAggregates(header, cm, r);
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
