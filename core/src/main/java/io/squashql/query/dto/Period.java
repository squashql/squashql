package io.squashql.query.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Set;

/**
 * TODO
 * MONTH_FROM_DATE,
 * QUARTER_FROM_DATE,
 * QUARTER_FROM_MONTH_YEAR,
 * SEMESTER_FROM_DATE,
 * YEAR_FROM_DATE
 */
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Period {

  @JsonIgnore
  Set<String> getFields();

  record Month(String month, String year) implements Period {

    @Override
    public Set<String> getFields() {
      return Set.of(this.month, this.year);
    }
  }

  record Quarter(String quarter, String year) implements Period {

    @Override
    public Set<String> getFields() {
      return Set.of(this.quarter, this.year);
    }
  }

  record QuarterFromDate(String date) implements Period {

    public String year() {
      return "YEAR(" + date + ")";
    }

    public String quarter() {
      return "QUARTER(" + date + ")";
    }

    @Override
    public Set<String> getFields() {
      return Set.of(this.date);
    }
  }

  interface ReferenceDatePosition {
  }

  record ReferenceQuarterFromDatePosition(QuarterFromDate quarter, int quarterShift, int yearShift) implements ReferenceDatePosition {
  }

  static ReferenceDatePosition fromQuarterFromDate(QuarterFromDate quarter, int quarterShift, int yearShift) {
    return new ReferenceQuarterFromDatePosition(quarter, quarterShift, yearShift);
  }

  record Semester(String semester, String year) implements Period {

    @Override
    public Set<String> getFields() {
      return Set.of(this.semester, this.year);
    }
  }

  record Year(String year) implements Period {

    @Override
    public Set<String> getFields() {
      return Set.of(this.year);
    }
  }
}
