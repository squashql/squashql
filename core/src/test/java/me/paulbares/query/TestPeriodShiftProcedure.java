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
  void testQuarterFromQuarterYear() {
    Period period = new Period.Quarter("", "");
    MutableObjectIntMap<PeriodUnit> indexByPeriodUnit = new ObjectIntHashMap<>();
    indexByPeriodUnit.put(PeriodUnit.YEAR, 0);
    indexByPeriodUnit.put(PeriodUnit.QUARTER, 1);
    BiFunction<Object[], String[], Object[]> f = (point, refPos) -> {
      new PeriodComparisonExecutor.ShiftProcedure(
              period,
              Map.of(PeriodUnit.YEAR, refPos[0], PeriodUnit.QUARTER, refPos[1]),
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

  @Test
  void testMonthFromMonthYear() {
    Period period = new Period.Month("", "");
    MutableObjectIntMap<PeriodUnit> indexByPeriodUnit = new ObjectIntHashMap<>();
    indexByPeriodUnit.put(PeriodUnit.YEAR, 0);
    indexByPeriodUnit.put(PeriodUnit.MONTH, 1);
    BiFunction<Object[], String[], Object[]> f = (point, refPos) -> {
      new PeriodComparisonExecutor.ShiftProcedure(
              period,
              Map.of(PeriodUnit.YEAR, refPos[0], PeriodUnit.MONTH, refPos[1]),
              indexByPeriodUnit).test(point);
      return point;
    };

    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y", "q-1"})).containsExactly(2021, 12);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y", "q-1"})).containsExactly(2022, 1);
    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y-1", "q-1"})).containsExactly(2020, 12);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y-1", "q-1"})).containsExactly(2021, 1);

    Assertions.assertThat(f.apply(new Object[]{2022, 12}, new String[]{"y", "q+1"})).containsExactly(2023, 1);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y", "q+1"})).containsExactly(2022, 3);
    Assertions.assertThat(f.apply(new Object[]{2022, 12}, new String[]{"y+1", "q+1"})).containsExactly(2024, 1);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y+1", "q+1"})).containsExactly(2023, 3);

    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y", "q-2"})).containsExactly(2021, 11);
  }

  @Test
  void testSemesterFromSemesterYear() {
    Period period = new Period.Semester("", "");
    MutableObjectIntMap<PeriodUnit> indexByPeriodUnit = new ObjectIntHashMap<>();
    indexByPeriodUnit.put(PeriodUnit.YEAR, 0);
    indexByPeriodUnit.put(PeriodUnit.SEMESTER, 1);
    BiFunction<Object[], String[], Object[]> f = (point, refPos) -> {
      new PeriodComparisonExecutor.ShiftProcedure(
              period,
              Map.of(PeriodUnit.YEAR, refPos[0], PeriodUnit.SEMESTER, refPos[1]),
              indexByPeriodUnit).test(point);
      return point;
    };

    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y", "q-1"})).containsExactly(2021, 2);
    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y", "q-2"})).containsExactly(2021, 1);
    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y", "q-3"})).containsExactly(2020, 2);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y-1", "q-1"})).containsExactly(2021, 1);
    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y-1", "q-1"})).containsExactly(2020, 2);

    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y", "q+1"})).containsExactly(2022, 2);
    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y", "q+2"})).containsExactly(2023, 1);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y+1", "q+1"})).containsExactly(2024, 1);
    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y+1", "q+1"})).containsExactly(2023, 2);
  }
}
