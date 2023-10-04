package io.squashql.query;

import io.squashql.query.database.SQLTranslator;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;

public class ParentComparisonExecutor extends AComparisonExecutor {

  final ComparisonMeasureReferencePosition pcm;

  public ParentComparisonExecutor(ComparisonMeasureReferencePosition pcm) {
    this.pcm = pcm;
  }

  @Override
  protected BiPredicate<Object[], Header[]> createShiftProcedure(ComparisonMeasureReferencePosition cm, ObjectIntMap<String> indexByColumn) {
    List<Field> ancestors = new ArrayList<>(this.pcm.ancestors);
    Collections.reverse(ancestors);
    return new ShiftProcedure(ancestors, indexByColumn);
  }

  static class ShiftProcedure implements BiPredicate<Object[], Header[]> {

    final List<Field> ancestors;
    final ObjectIntMap<String> indexByColumn;

    ShiftProcedure(List<Field> ancestors, ObjectIntMap<String> indexByColumn) {
      this.ancestors = ancestors;
      this.indexByColumn = indexByColumn;
    }

    @Override
    public boolean test(Object[] row, Header[] headers) {
      for (Field ancestor : this.ancestors) {
        // Is it expressed ?
        String name = ancestor.name();
        if (this.indexByColumn.containsKey(name)) {
          int index = this.indexByColumn.get(name);
          if (!row[index].equals(SQLTranslator.TOTAL_CELL)) {
            row[index] = SQLTranslator.TOTAL_CELL;
            break;
          }
        }
      }
      return true;
    }
  }
}
