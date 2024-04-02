package io.squashql.util;

import io.squashql.query.*;
import io.squashql.query.database.SqlUtils;
import io.squashql.query.dto.*;
import io.squashql.query.measure.ParametrizedMeasure;
import io.squashql.table.PivotTableUtils;

import java.util.*;

import static io.squashql.query.dto.OrderKeywordDto.DESC;

public final class Queries {

  // Suppresses default constructor, ensuring non-instantiability.
  private Queries() {
  }

  public static Map<String, Comparator<?>> getComparators(QueryDto queryDto) {
    Map<Field, OrderDto> orders = queryDto.orders;
    Map<String, Comparator<?>> res = new LinkedHashMap<>(); // order is important !
    orders.forEach((c, order) -> {
      if (order instanceof SimpleOrderDto so) {
        res.put(SqlUtils.squashqlExpression(c), NullAndTotalComparator.nullsLastAndTotalsFirst(so.order == DESC ? Comparator.naturalOrder().reversed() : Comparator.naturalOrder()));
      } else if (order instanceof ExplicitOrderDto eo) {
        res.put(SqlUtils.squashqlExpression(c), NullAndTotalComparator.nullsLastAndTotalsFirst(new CustomExplicitOrdering(eo.explicit)));
      } else {
        throw new IllegalStateException("Unexpected value: " + orders);
      }
    });

    // Special case for group that defines implicitly an order.
    ColumnSet group = queryDto.columnSets.get(ColumnSetKey.GROUP);
    if (group != null) {
      GroupColumnSetDto cs = (GroupColumnSetDto) group;
      Map<Object, List<Object>> m = new LinkedHashMap<>();
      cs.values.forEach((k, v) -> {
        List<Object> l = new ArrayList<>(v);
        m.put(k, l);
      });
      res.put(SqlUtils.squashqlExpression(cs.newField), new CustomExplicitOrdering(new ArrayList<>(m.keySet())));
      res.put(SqlUtils.squashqlExpression(cs.field), DependentExplicitOrdering.create(m));
    }

    return res;
  }

  public record PartialMeasureVisitor(
          PivotTableUtils.PivotTableContext pivotTableContext) implements MeasureVisitor<Measure> {

    @Override
    public Measure visit(AggregatedMeasure measure) {
      return measure;
    }

    @Override
    public Measure visit(ExpressionMeasure measure) {
      return measure;
    }

    @Override
    public Measure visit(BinaryOperationMeasure measure) {
      return new BinaryOperationMeasure(measure.alias,
              measure.operator,
              measure.leftOperand.accept(this),
              measure.rightOperand.accept(this));
    }

    @Override
    public Measure visit(ComparisonMeasureReferencePosition measure) {
      return null;
    }

    @Override
    public Measure visit(ComparisonMeasureGrandTotal measure) {
      return null;
    }

    @Override
    public Measure visit(DoubleConstantMeasure measure) {
      return measure;
    }

    @Override
    public Measure visit(LongConstantMeasure measure) {
      return measure;
    }

    @Override
    public Measure visit(VectorAggMeasure measure) {
      return null;
    }

    @Override
    public Measure visit(VectorTupleAggMeasure measure) {
      return null;
    }

    @Override
    public Measure visit(ParametrizedMeasure measure) {
      return measure;
    }

    @Override
    public Measure visit(PartialComparisonAncestorsMeasure measure) {
      List<Field> ancestors = switch (measure.axis) {
        case ROW -> this.pivotTableContext.cleansedRows;
        case COLUMN -> this.pivotTableContext.cleansedColumns;
      };
      return new ComparisonMeasureReferencePosition(
              measure.alias,
              measure.comparisonMethod,
              measure.measure.accept(this),
              ancestors
      );
    }
  }
}
