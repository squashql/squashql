package io.squashql.query.compiled;


import io.squashql.type.TypedField;

import java.util.Set;

public interface CompiledPeriod {

  Set<TypedField> getTypedFields();

  record Month(TypedField month, TypedField year) implements CompiledPeriod {

    @Override
    public Set<TypedField> getTypedFields() {
      return Set.of(this.month, this.year);
    }
  }

  record Quarter(TypedField quarter, TypedField year) implements CompiledPeriod {

    @Override
    public Set<TypedField> getTypedFields() {
      return Set.of(this.quarter, this.year);
    }
  }

  record Semester(TypedField semester, TypedField year) implements CompiledPeriod {

    @Override
    public Set<TypedField> getTypedFields() {
      return Set.of(this.semester, this.year);
    }
  }

  record Year(TypedField year) implements CompiledPeriod {

    @Override
    public Set<TypedField> getTypedFields() {
      return Set.of(this.year);
    }
  }
}
