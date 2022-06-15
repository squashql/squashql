package me.paulbares.query;

import me.paulbares.query.dto.Period;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.BiFunction;

public class TestPeriodBucketingExecutor {

  @Test
  void testQuarterFromMonthYear() {
    Period period = new Period.QuarterFromMonthYear("", "");

    BiFunction<Object[], String[], Object[]> f = (point, refPos) -> PeriodBucketingExecutor
            .computeNewPositionFromReferencePosition(period,
                    point,
                    new ComparisonMeasure.PeriodUnit[]{ComparisonMeasure.PeriodUnit.YEAR, ComparisonMeasure.PeriodUnit.QUARTER},
                    Map.of(ComparisonMeasure.PeriodUnit.YEAR, refPos[0], ComparisonMeasure.PeriodUnit.QUARTER, refPos[1]));


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
