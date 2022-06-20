package me.paulbares.query.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;

/**
 * TODO
 * MONTH,
 * MONTH_FROM_DATE,
 * QUARTER,
 * QUARTER_FROM_DATE,
 * QUARTER_FROM_MONTH_YEAR,
 * SEMESTER,
 * SEMESTER_FROM_DATE,
 * YEAR_FROM_DATE
 */
public interface Period {

  @JsonIgnore
  String getJsonKey();

  record Quarter(String quarter, String year) implements Period {
    public static final String JSON_KEY = "quarter";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }
  }

  record QuarterFromDate(String date) implements Period {
    public static final String JSON_KEY = "quarter_from_date";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }
  }

  record QuarterFromMonthYear(String month, String year) implements Period {
    public static final String JSON_KEY = "quarter_from_month_year";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }
  }

  record Semester(String semester) implements Period {
    public static final String JSON_KEY = "semester";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }
  }

  record SemesterFromDate(String date) implements Period {
    public static final String JSON_KEY = "semester_from_date";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }
  }

  record YearFromDate(String date) implements Period {
    public static final String JSON_KEY = "year_from_date";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }
  }

  record Year(String year) implements Period {
    public static final String JSON_KEY = "year";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }
  }
}
