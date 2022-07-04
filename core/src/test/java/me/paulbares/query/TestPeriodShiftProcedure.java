package me.paulbares.query;

import me.paulbares.query.dto.Period;
import org.assertj.core.api.Assertions;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.BiFunction;

public class TestPeriodShiftProcedure {

  @Test
  void testQuarterFromMonthYear() {
    Period period = new Period.Quarter("", "");
    MutableObjectIntMap<ComparisonMeasure.PeriodUnit> indexByPeriodUnit = new ObjectIntHashMap<>();
    indexByPeriodUnit.put(ComparisonMeasure.PeriodUnit.YEAR, 0);
    indexByPeriodUnit.put(ComparisonMeasure.PeriodUnit.QUARTER, 1);
    BiFunction<Object[], String[], Object[]> f = (point, refPos) -> {
      new PeriodComparisonExecutor.ShiftProcedure(
              period,
              Map.of(ComparisonMeasure.PeriodUnit.YEAR, refPos[0], ComparisonMeasure.PeriodUnit.QUARTER, refPos[1]),
              indexByPeriodUnit).test(point);
      return point;
    };

    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y", "q-1"})).containsExactly(2021, 4);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y", "q-1"})).containsExactly(2022, 1);
    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y-1", "q-1"})).containsExactly(2020, 4);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y-1", "q-1"})).containsExactly(2021, 1);

    Assertions.assertThat(f.apply(new Object[]{2022, 4}, new String[]{"y", "q+1"})).containsExactly(2023, 1);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y", "q+1"})).containsExactly(2022, 3);
    Assertions.assertThat(f.apply(new Object[]{2022, 4}, new String[]{"y+1", "q+1"})).containsExactly(2024, 1);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y+1", "q+1"})).containsExactly(2023, 3);

    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y", "q-2"})).containsExactly(2021, 3);
  }
}
