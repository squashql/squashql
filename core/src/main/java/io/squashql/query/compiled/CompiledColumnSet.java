package io.squashql.query.compiled;

import io.squashql.query.ColumnSetKey;
import io.squashql.type.TypedField;

import java.util.List;

public interface CompiledColumnSet {

  List<TypedField> columnsForPrefetching();

  List<TypedField> newColumns();

  ColumnSetKey columnSetKey();
}
