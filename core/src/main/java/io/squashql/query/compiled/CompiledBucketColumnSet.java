package io.squashql.query.compiled;

import io.squashql.query.ColumnSetKey;
import io.squashql.type.TypedField;

import java.util.List;
import java.util.Map;

public record CompiledBucketColumnSet(List<TypedField> columnsForPrefetching,
                                      List<TypedField> newColumns,
                                      ColumnSetKey columnSetKey,
                                      Map<String, List<String>> values) implements CompiledColumnSet {

  public TypedField newField() {
    return this.newColumns.get(0);
  }

  public TypedField field() {
    return this.columnsForPrefetching.get(0);
  }
}
