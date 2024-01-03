package io.squashql.query.compiled;

import io.squashql.query.*;
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

import static io.squashql.query.ColumnSetKey.BUCKET;

public class Evaluator implements BiConsumer<QueryPlanNodeKey, ExecutionContext>, MeasureVisitor<Void> {

  private ExecutionContext executionContext;

  @Override
  public void accept(QueryPlanNodeKey queryPlanNodeKey, ExecutionContext executionContext) {
    CompiledMeasure measure = queryPlanNodeKey.measure();
    if (executionContext.getWriteToTable().measures().contains(measure)) {
      return; // Nothing to do
    }
    this.executionContext = executionContext;
    measure.accept(this);
  }

  @Override
  public Void visit(CompiledBinaryOperationMeasure bom) {
    Table intermediateResult = this.executionContext.getWriteToTable();
    List<Object> lo = intermediateResult.getAggregateValues(bom.leftOperand());
    List<Object> ro = intermediateResult.getAggregateValues(bom.rightOperand());
    List<Object> r = new ArrayList<>(lo.size());

    Class<?> lType = intermediateResult.getHeader(bom.leftOperand()).type();
    Class<?> rType = intermediateResult.getHeader(bom.rightOperand()).type();
    BiFunction<Number, Number, Number> operation = BinaryOperations.createBiFunction(bom.operator(), lType, rType);
    for (int i = 0; i < lo.size(); i++) {
      r.add(operation.apply((Number) lo.get(i), (Number) ro.get(i)));
    }
    Header header = new Header(bom.alias(), BinaryOperations.getOutputType(bom.operator(), lType, rType), true);
    intermediateResult.addAggregates(header, bom, r);
    return null;
  }

  @Override
  public Void visit(CompiledComparisonMeasure cm) {
    AComparisonExecutor executor;
    if (cm.columnSetKey() == BUCKET) {
      CompiledColumnSet cs = this.executionContext.columnSets().get(BUCKET);
      if (cs == null) {
        throw new IllegalArgumentException(String.format("columnSet %s is not specified in the query but is used in a comparison measure: %s", BUCKET, cm));
      }
      executor = new BucketComparisonExecutor((CompiledBucketColumnSet) cs);
    } else if (cm.period() != null) {
      for (TypedField field : cm.period().getTypedFields()) {
        if (!this.executionContext.columns().contains(field)) {
          throw new IllegalArgumentException(String.format("%s is not specified in the query but is used in a comparison measure: %s", field.name(), cm));
        }
      }
      executor = new PeriodComparisonExecutor(cm);
    } else if (cm.ancestors() != null) {
      executor = new ParentComparisonExecutor(cm);
    } else {
      throw new IllegalArgumentException(String.format("Comparison measure not correctly defined (%s). It should have a period or columnSetKey parameter", cm));
    }

    QueryExecutor.QueryScope readScope = MeasureUtils.getReadScopeComparisonMeasureReferencePosition(
            this.executionContext.columns(), this.executionContext.bucketColumns(), cm, this.executionContext.queryScope());
    Table readFromTable = this.executionContext.tableByScope().get(readScope); // Table where to read the aggregates
    if (readFromTable.count() == this.executionContext.queryLimit()) {
      throw new RuntimeException("Too many rows, some intermediate results exceed the limit " + this.executionContext.queryLimit());
    }
    executeComparator(cm, this.executionContext.getWriteToTable(), readFromTable, executor);
    return null;
  }

  private static void executeComparator(CompiledComparisonMeasure cm, Table writeToTable, Table readFromTable, AComparisonExecutor executor) {
    List<Object> agg = executor.compare(cm, writeToTable, readFromTable);
    Header header = new Header(cm.alias(), BinaryOperations.getComparisonOutputType(cm.comparisonMethod(), writeToTable.getHeader(cm.measure()).type()), true);
    writeToTable.addAggregates(header, cm, agg);
  }

  @Override
  public Void visit(CompiledDoubleConstantMeasure measure) {
    executeConstantOperation(measure, this.executionContext.getWriteToTable());
    return null;
  }

  @Override
  public Void visit(CompiledLongConstantMeasure measure) {
    executeConstantOperation(measure, this.executionContext.getWriteToTable());
    return null;
  }

  private static void executeConstantOperation(CompiledMeasure cm, Table intermediateResult) {
    Object v;
    Class<?> type;
    if (cm instanceof DoubleConstantMeasure dcm) {
      v = ((Number) dcm.value).doubleValue();
      type = double.class;
    } else if (cm instanceof LongConstantMeasure lcm) {
      v = ((Number) lcm.value).longValue();
      type = long.class;
    } else {
      throw new IllegalArgumentException("Unexpected type " + cm.getClass() + ". Only double and long are supported");
    }
    Header header = new Header(cm.alias(), type, true);
    List<Object> r = Collections.nCopies((int) intermediateResult.count(), v);
    intermediateResult.addAggregates(header, cm, r);
  }

  // The following measures are not evaluated here but in the underlying DB.

  @Override
  public Void visit(CompiledAggregatedMeasure measure) {
    throw new IllegalStateException(CompiledAggregatedMeasure.class.getSimpleName());
  }

  @Override
  public Void visit(CompiledExpressionMeasure measure) {
    throw new IllegalStateException(CompiledExpressionMeasure.class.getSimpleName());
  }

  @Override
  public Void visit(CompiledVectorAggMeasure measure) {
    // Retrieve the query scope use for the prefetch, the logic should be the same to retrieve the result.
    QueryExecutor.QueryScope prefetchQueryScope = new PrefetchVisitor(this.executionContext.columns(), this.executionContext.bucketColumns(), this.executionContext.queryScope())
            .visit(measure)
            .keySet()
            .iterator()
            .next();
//    Queries.modifyQueryLimit(this.executionContext.queryScope(), prefetchQueryScope); FIXME
    Table readTable = this.executionContext.tableByScope().get(prefetchQueryScope);
    Table writeToTable = this.executionContext.getWriteToTable();
    writeToTable.transferAggregates(readTable, measure);
    return null;
  }
}
