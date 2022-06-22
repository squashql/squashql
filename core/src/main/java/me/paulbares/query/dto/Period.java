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

  record Month(String month, String year) implements Period {
    public static final String JSON_KEY = "month";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }
  }

  record Quarter(String quarter, String year) implements Period {
    public static final String JSON_KEY = "quarter";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }
  }

  record Semester(String semester, String year) implements Period {
    public static final String JSON_KEY = "semester";

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
