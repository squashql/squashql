package me.paulbares.query.dto;

import me.paulbares.query.BinaryOperationMeasure.PeriodUnit;
import me.paulbares.query.ColumnSet;
import me.paulbares.store.Field;

import java.util.List;
import java.util.Map;

import static me.paulbares.query.BinaryOperationMeasure.PeriodUnit.QUARTER;
import static me.paulbares.query.BinaryOperationMeasure.PeriodUnit.YEAR;

public class PeriodColumnSetDto implements ColumnSet {

  public Period period;

  /**
   * For Jackson.
   */
  public PeriodColumnSetDto() {
  }

  public PeriodColumnSetDto(Period period) {
    this.period = period;
  }

  /**
   * Gets the column names to use for prefetching. It will determine which grouping of aggregates are needed to further
   * perform the bucketing.
   */
  @Override
  public List<String> getColumnsForPrefetching() {
    return getColumnsForPrefetching(this.period);
  }

  public static List<String> getColumnsForPrefetching(Period period) {
    if (period instanceof Period.Quarter q) {
      return List.of(q.year(), q.quarter());
    } else if (period instanceof Period.Year y) {
      return List.of(y.year());
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }

  public Map<String, PeriodUnit> mapping() {
    if (this.period instanceof Period.Quarter q) {
      return Map.of(q.quarter(), QUARTER, q.year(), YEAR);
    } else if (this.period instanceof Period.Year y) {
      return Map.of(y.year(), YEAR);
    } else {
      throw new RuntimeException(this.period + " not supported yet");
    }
  }

  /**
   * Gets the list of new fields that will appear in the final result table once the bucketing is done.
   */
  @Override
  public List<Field> getNewColumns() {
    return getNewColumns(this.period);
  }

  public static List<Field> getNewColumns(Period period) {
    if (period instanceof Period.Quarter q) {
      return List.of(new Field(q.year(), int.class), new Field(q.quarter(), int.class));
    } else if (period instanceof Period.Year y) {
      return List.of(new Field(y.year(), String.class));
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }
}
