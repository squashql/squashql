package io.squashql.query.measure;

import io.squashql.list.Lists;
import io.squashql.query.Field;
import io.squashql.query.FieldAndAggFunc;
import io.squashql.query.Measure;
import io.squashql.query.VectorTupleAggMeasure;
import io.squashql.util.MultipleColumnsSorter;
import lombok.*;

import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static io.squashql.query.agg.AggregationFunction.ANY_VALUE;
import static io.squashql.query.agg.AggregationFunction.SUM;
import static io.squashql.util.ListUtils.reorder;
import static java.util.Comparator.naturalOrder;

@ToString
@EqualsAndHashCode
@NoArgsConstructor // For Jackson
@AllArgsConstructor
public class Var implements Measure {

  public String alias;
  public Field value;
  public Field date;
  public double quantile;
  @With
  public String expression;

  @Override
  public String alias() {
    return this.alias;
  }

  @Override
  public String expression() {
    return this.expression;
  }

  Measure var() {
    Function<List<Object>, Object> transformer = (tuple) -> {
      Lists.DoubleList prices = (Lists.DoubleList) tuple.get(0);
      Lists.LocalDateList dates = (Lists.LocalDateList) tuple.get(1);
      int[] sort = MultipleColumnsSorter.sort(Collections.singletonList(dates), Collections.singletonList(naturalOrder()), new int[0]);

      List<Double> orderedPrices = reorder(prices, sort);
      List<LocalDate> orderedDates = reorder(dates, sort);
      var index = (int) Math.floor(orderedPrices.size() * (1 - this.quantile));
      var quantileDate = orderedDates.get(index);
      var quantilePnL = orderedPrices.get(index);

      return List.of(quantileDate, quantilePnL);
    };
    return new VectorTupleAggMeasure(this.alias, List.of(new FieldAndAggFunc(this.value, SUM), new FieldAndAggFunc(this.date, ANY_VALUE)), this.date, transformer);
  }
}
