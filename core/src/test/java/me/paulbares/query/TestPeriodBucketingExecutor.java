package me.paulbares.query;

import me.paulbares.query.dto.Period;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.function.Function;

public class TestPeriodBucketingExecutor {

  @Test
  void test() {
    Period period = new Period.QuarterFromMonthYear("", "");

    Function<Object[], Object[]> f = o -> PeriodBucketingExecutor
            .computeNewPositionFromReferencePosition(period,
                    o,
                    new ComparisonMeasure.PeriodUnit[]{ComparisonMeasure.PeriodUnit.YEAR, ComparisonMeasure.PeriodUnit.QUARTER},
                    Map.of(
                            ComparisonMeasure.PeriodUnit.YEAR, "y",
                            ComparisonMeasure.PeriodUnit.QUARTER, "q-1"));


    Assertions.assertThat(f.apply(new Object[]{2022, 1})).containsExactly(2021, 4);
    Assertions.assertThat(f.apply(new Object[]{2022, 2})).containsExactly(2022, 1);
  }
}
