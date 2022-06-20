package me.paulbares.query.dto;

import me.paulbares.query.ColumnSet;
import me.paulbares.store.Field;

import java.util.List;

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
    if (period instanceof Period.QuarterFromMonthYear q) {
      return List.of(q.year(), q.month());
    } else if (period instanceof Period.QuarterFromDate q) {
      return List.of(q.date());
    } else if (period instanceof Period.YearFromDate y) {
      return List.of(y.date());
    } else if (period instanceof Period.Year y) {
      return List.of(y.year());
    } else {
      throw new RuntimeException(period + " not supported yet");
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
    if (period instanceof Period.QuarterFromMonthYear q) {
      return List.of(new Field(q.year(), String.class), new Field("quarter", String.class));
    } else if (period instanceof Period.QuarterFromDate) {
      return List.of(new Field("year", String.class), new Field("quarter", String.class));
    } else if (period instanceof Period.YearFromDate) {
      return List.of(new Field("year", String.class));
    } else if (period instanceof Period.Year y) {
      return List.of(new Field(y.year(), String.class));
    } else {
      throw new RuntimeException(period + " not supported yet");
    }
  }
}
