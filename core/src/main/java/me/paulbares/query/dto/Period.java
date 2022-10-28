package me.paulbares.query.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.Set;

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
@JsonTypeInfo(use = JsonTypeInfo.Id.CLASS)
public interface Period {

  @JsonIgnore
  String getJsonKey();

  @JsonIgnore
  Set<String> getFields();

  record Month(String month, String year) implements Period {
    public static final String JSON_KEY = "month";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }

    @Override
    public Set<String> getFields() {
      return Set.of(this.month, this.year);
    }
  }

  record Quarter(String quarter, String year) implements Period {
    public static final String JSON_KEY = "quarter";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }

    @Override
    public Set<String> getFields() {
      return Set.of(this.quarter, this.year);
    }
  }

  record Semester(String semester, String year) implements Period {
    public static final String JSON_KEY = "semester";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }

    @Override
    public Set<String> getFields() {
      return Set.of(this.semester, this.year);
    }
  }

  record Year(String year) implements Period {
    public static final String JSON_KEY = "year";

    @Override
    public String getJsonKey() {
      return JSON_KEY;
    }

    @Override
    public Set<String> getFields() {
      return Set.of(this.year);
    }
  }
}
