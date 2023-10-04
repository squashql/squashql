package io.squashql.query;

import static io.squashql.query.TableField.tableField;

import io.squashql.query.dto.Period;
import java.util.Map;
import java.util.function.BiFunction;
import org.assertj.core.api.Assertions;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.junit.jupiter.api.Test;

public class TestPeriodShiftProcedure {

  @Test
  void testQuarterFromQuarterYear() {
    Period period = new Period.Quarter(tableField(""), tableField(""));
    MutableObjectIntMap<PeriodUnit> indexByPeriodUnit = new ObjectIntHashMap<>();
    indexByPeriodUnit.put(PeriodUnit.YEAR, 0);
    indexByPeriodUnit.put(PeriodUnit.QUARTER, 1);
    BiFunction<Object[], String[], Object[]> f = (point, refPos) -> {
      new PeriodComparisonExecutor.ShiftProcedure(
              period,
              Map.of(PeriodUnit.YEAR, refPos[0], PeriodUnit.QUARTER, refPos[1]),
              indexByPeriodUnit).test(point, new Header[]{new Header("year", int.class, false), new Header("quarter", int.class, false)});
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
    Period period = new Period.Month(tableField(""), tableField(""));
    MutableObjectIntMap<PeriodUnit> indexByPeriodUnit = new ObjectIntHashMap<>();
    indexByPeriodUnit.put(PeriodUnit.YEAR, 0);
    indexByPeriodUnit.put(PeriodUnit.MONTH, 1);
    BiFunction<Object[], String[], Object[]> f = (point, refPos) -> {
      new PeriodComparisonExecutor.ShiftProcedure(
              period,
              Map.of(PeriodUnit.YEAR, refPos[0], PeriodUnit.MONTH, refPos[1]),
              indexByPeriodUnit).test(point, new Header[]{new Header("year", long.class, false), new Header("month", long.class, false)}); // use long and make sure we get long at the end
      return point;
    };

    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y", "q-1"})).containsExactly(2021l, 12l);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y", "q-1"})).containsExactly(2022l, 1l);
    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y-1", "q-1"})).containsExactly(2020l, 12l);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y-1", "q-1"})).containsExactly(2021l, 1l);

    Assertions.assertThat(f.apply(new Object[]{2022, 12}, new String[]{"y", "q+1"})).containsExactly(2023l, 1l);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y", "q+1"})).containsExactly(2022l, 3l);
    Assertions.assertThat(f.apply(new Object[]{2022, 12}, new String[]{"y+1", "q+1"})).containsExactly(2024l, 1l);
    Assertions.assertThat(f.apply(new Object[]{2022, 2}, new String[]{"y+1", "q+1"})).containsExactly(2023l, 3l);

    Assertions.assertThat(f.apply(new Object[]{2022, 1}, new String[]{"y", "q-2"})).containsExactly(2021l, 11l);
  }

  @Test
  void testSemesterFromSemesterYear() {
    Period period = new Period.Semester(tableField(""), tableField(""));
    MutableObjectIntMap<PeriodUnit> indexByPeriodUnit = new ObjectIntHashMap<>();
    indexByPeriodUnit.put(PeriodUnit.YEAR, 0);
    indexByPeriodUnit.put(PeriodUnit.SEMESTER, 1);
    BiFunction<Object[], String[], Object[]> f = (point, refPos) -> {
      new PeriodComparisonExecutor.ShiftProcedure(
              period,
              Map.of(PeriodUnit.YEAR, refPos[0], PeriodUnit.SEMESTER, refPos[1]),
              indexByPeriodUnit).test(point, new Header[]{new Header("year", int.class, false), new Header("semester", int.class, false)});
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
