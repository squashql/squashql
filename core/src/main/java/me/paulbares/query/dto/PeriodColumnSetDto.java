package me.paulbares.query.dto;

import com.fasterxml.jackson.annotation.*;
import me.paulbares.query.ComparisonMeasure.PeriodUnit;
import me.paulbares.query.ColumnSet;
import me.paulbares.store.Field;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static me.paulbares.query.ComparisonMeasure.PeriodUnit.QUARTER;
import static me.paulbares.query.ComparisonMeasure.PeriodUnit.YEAR;

public class PeriodColumnSetDto implements ColumnSet {

//  @JsonTypeInfo(include = JsonTypeInfo.As.WRAPPER_OBJECT, use = JsonTypeInfo.Id.NAME)
//  @JsonSubTypes({
//          @JsonSubTypes.Type(value = Period.Month.class, name = Period.Quarter.JSON_KEY),
//          @JsonSubTypes.Type(value = Period.Quarter.class, name = Period.Quarter.JSON_KEY),
//          @JsonSubTypes.Type(value = Period.Semester.class, name = Period.Semester.JSON_KEY),
//          @JsonSubTypes.Type(value = Period.Year.class, name = Period.Year.JSON_KEY),
//  })
//  @JsonProperty
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
  @JsonIgnore
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
  @JsonIgnore
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

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    PeriodColumnSetDto that = (PeriodColumnSetDto) o;
    return Objects.equals(this.period, that.period);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.period);
  }
}
