package io.squashql.query.compiled;


import io.squashql.type.NamedTypedField;

import java.util.Set;

public interface CompiledPeriod {

  Set<NamedTypedField> getTypedFields();

  record Month(NamedTypedField month, NamedTypedField year) implements CompiledPeriod {

    @Override
    public Set<NamedTypedField> getTypedFields() {
      return Set.of(this.month, this.year);
    }
  }

  record Quarter(NamedTypedField quarter, NamedTypedField year) implements CompiledPeriod {

    @Override
    public Set<NamedTypedField> getTypedFields() {
      return Set.of(this.quarter, this.year);
    }
  }

  record Semester(NamedTypedField semester, NamedTypedField year) implements CompiledPeriod {

    @Override
    public Set<NamedTypedField> getTypedFields() {
      return Set.of(this.semester, this.year);
    }
  }

  record Year(NamedTypedField year) implements CompiledPeriod {

    @Override
    public Set<NamedTypedField> getTypedFields() {
      return Set.of(this.year);
    }
  }
}
