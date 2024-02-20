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

public final class Repository {

  public static final String VAR = "VAR";
  public static final String INCREMENTAL_VAR = "INCREMENTAL_VAR";

  private Repository() {
  }

  public static Measure create(ParametrizedMeasure m) {
    if (m.key.equals(VAR)) {
      return var(m.alias, get(m, "value"), get(m, "date"), get(m, "quantile"));
    } else if (m.key.equals(INCREMENTAL_VAR)) {
      return incrementalVar(m.alias, get(m, "value"), get(m, "date"), get(m, "quantile"), get(m, "ancestors"));
    } else {
      throw new IllegalArgumentException("unknown " + ParametrizedMeasure.class + ": " + m);
    }
  }

  private static <T> T get(ParametrizedMeasure m, String field) {
    return (T) m.parameters.get(field);
  }

  private static Measure var(String alias, Field value, Field date, double quantile) {
    Function<List<Object>, Object> transformer = (tuple) -> {
      Lists.DoubleList prices = (Lists.DoubleList) tuple.get(0);
      Lists.LocalDateList dates = (Lists.LocalDateList) tuple.get(1);
      int[] sort = MultipleColumnsSorter.sort(Collections.singletonList(dates), Collections.singletonList(naturalOrder()), new int[0]);

      List<Double> orderedPrices = reorder(prices, sort);
      List<LocalDate> orderedDates = reorder(dates, sort);
      var index = (int) Math.floor(orderedPrices.size() * (1 - quantile));
      var quantileDate = orderedDates.get(index);
      var quantilePnL = orderedPrices.get(index);

      return List.of(quantileDate, quantilePnL);
    };
    return new VectorTupleAggMeasure(alias, List.of(new FieldAndAggFunc(value, SUM), new FieldAndAggFunc(date, ANY_VALUE)), date, transformer);
  }

  private static Measure incrementalVar(String alias, Field value, Field date, double quantile, List<Field> ancestors) {
    Measure vector = new VectorTupleAggMeasure("__vector__",
            List.of(new FieldAndAggFunc(value, SUM),
                    new FieldAndAggFunc(date, ANY_VALUE)),
            date,
            null);

    BiFunction<Object, Object, Object> comparisonOperator = (currentValue, parentValue) -> {
      List<Double> current = orderTupleOfList(currentValue).getOne();
      List<Double> parent = orderTupleOfList(parentValue).getOne();

      int size = parent.size();
      var index = (int) Math.floor(size * (1 - quantile));
      var varParentWithCurrent = parent.get(index);
      List<Double> minus = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        minus.add(parent.get(0) - current.get(0));
      }

      Collections.sort(minus);
      var varParentWithoutCurrent = minus.get(index);

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
