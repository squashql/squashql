package io.squashql.query.measure;

import io.squashql.list.Lists;
import io.squashql.query.*;
import io.squashql.util.MultipleColumnsSorter;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;

import static io.squashql.query.agg.AggregationFunction.ANY_VALUE;
import static io.squashql.query.agg.AggregationFunction.SUM;
import static io.squashql.util.ListUtils.reorder;
import static java.util.Comparator.naturalOrder;

public final class ParametrizedMeasureFactory {

  private ParametrizedMeasureFactory() {
  }

  /**
   * Called by reflection.
   */
  public static Measure var(String alias, Field value, Field date, double quantile) {
    Function<List<Object>, Object> transformer = (tuple) -> {
      Lists.DoubleList prices = (Lists.DoubleList) tuple.get(0);
      Lists.LocalDateList dates = (Lists.LocalDateList) tuple.get(1);
      int[] sort = MultipleColumnsSorter.sort(Collections.singletonList(prices), Collections.singletonList(naturalOrder()), new int[0]);

      List<Double> orderedPrices = reorder(prices, sort);
      List<LocalDate> orderedDates = reorder(dates, sort);
      var index = (int) Math.floor(orderedPrices.size() * (1 - quantile));
      var quantileDate = orderedDates.get(index);
      var quantilePnL = orderedPrices.get(index);

      return List.of(quantileDate, quantilePnL);
    };
    return new VectorTupleAggMeasure(alias, List.of(new FieldAndAggFunc(value, SUM), new FieldAndAggFunc(date, ANY_VALUE)), date, transformer);
  }

  /**
   * Called by reflection.
   */
  public static Measure incrementalVar(String alias, Field value, Field date, double quantile, List<Field> ancestors) {
    Measure vector = new VectorTupleAggMeasure("__vector__",
            List.of(new FieldAndAggFunc(value, SUM),
                    new FieldAndAggFunc(date, ANY_VALUE)),
            date,
            null);

    BiFunction<Object, Object, Object> comparisonOperator = (currentValue, parentValue) -> {
      if (currentValue == null || parentValue == null) {
        return null;
      }
      List<Double> current = orderTupleOfList(currentValue).getOne();
      List<Double> parent = orderTupleOfList(parentValue).getOne();

      int size = parent.size();
      var index = (int) Math.floor(size * (1 - quantile));
      List<Double> minus = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        minus.add(parent.get(i) - current.get(i));
      }

      Collections.sort(minus);
      var varParentWithoutCurrent = minus.get(index);
      List<Double> parentVector = new ArrayList<>(parent);
      Collections.sort(parentVector);
      var varParentWithCurrent = parentVector.get(index);

      return varParentWithCurrent - varParentWithoutCurrent;
    };
    return new ComparisonMeasureReferencePosition(alias, comparisonOperator, vector, ancestors);
  }

  private static Pair<List<Double>, List<LocalDate>> orderTupleOfList(Object value) {
    Lists.DoubleList prices = (Lists.DoubleList) ((List) value).get(0);
    Lists.LocalDateList dates = (Lists.LocalDateList) ((List) value).get(1);
    int[] sort = MultipleColumnsSorter.sort(Collections.singletonList(dates), Collections.singletonList(naturalOrder()), new int[0]);
    return Tuples.pair(reorder(prices, sort), reorder(dates, sort));
  }
}
