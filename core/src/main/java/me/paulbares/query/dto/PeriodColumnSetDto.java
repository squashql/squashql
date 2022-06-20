package me.paulbares.query.dto;

import me.paulbares.query.ColumnSet;

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
}
