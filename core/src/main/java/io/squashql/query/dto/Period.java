package io.squashql.query.dto;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import io.squashql.query.field.Field;
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
  Set<Field> getFields();

  record Month(Field month, Field year) implements Period {

    @Override
    public Set<Field> getFields() {
      return Set.of(this.month, this.year);
    }
  }

  record Quarter(Field quarter, Field year) implements Period {

    @Override
    public Set<Field> getFields() {
      return Set.of(this.quarter, this.year);
    }
  }

  record Semester(Field semester, Field year) implements Period {

    @Override
    public Set<Field> getFields() {
      return Set.of(this.semester, this.year);
    }
  }

  record Year(Field year) implements Period {

    @Override
    public Set<Field> getFields() {
      return Set.of(this.year);
    }
  }
}
