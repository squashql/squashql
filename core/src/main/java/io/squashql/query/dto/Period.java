package io.squashql.query.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.squashql.query.NamedField;

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
  Set<NamedField> getNamedFields();

  record Month(NamedField month, NamedField year) implements Period {

    @Override
    public Set<NamedField> getNamedFields() {
      return Set.of(this.month, this.year);
    }
  }

  record Quarter(NamedField quarter, NamedField year) implements Period {

    @Override
    public Set<NamedField> getNamedFields() {
      return Set.of(this.quarter, this.year);
    }
  }

  record Semester(NamedField semester, NamedField year) implements Period {

    @Override
    public Set<NamedField> getNamedFields() {
      return Set.of(this.semester, this.year);
    }
  }

  record Year(NamedField year) implements Period {

    @Override
    public Set<NamedField> getNamedFields() {
      return Set.of(this.year);
    }
  }
}
