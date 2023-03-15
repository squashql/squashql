package io.squashql.query;

import io.squashql.query.database.SQLTranslator;
import io.squashql.store.FieldWithStore;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;

import java.util.List;
import java.util.function.BiPredicate;

public class ParentComparisonExecutor extends AComparisonExecutor {

  final ComparisonMeasureReferencePosition pcm;

  public ParentComparisonExecutor(ComparisonMeasureReferencePosition pcm) {
    this.pcm = pcm;
  }

  @Override
  protected BiPredicate<Object[], FieldWithStore[]> createShiftProcedure(ComparisonMeasureReferencePosition cm, ObjectIntMap<String> indexByColumn) {
    return new ShiftProcedure(this.pcm.ancestors, indexByColumn);
  }

  static class ShiftProcedure implements BiPredicate<Object[], FieldWithStore[]> {

    final List<String> ancestors;
    final ObjectIntMap<String> indexByColumn;

    ShiftProcedure(List<String> ancestors, ObjectIntMap<String> indexByColumn) {
      this.ancestors = ancestors;
      this.indexByColumn = indexByColumn;
    }

    @Override
    public boolean test(Object[] row, FieldWithStore[] fields) {
      for (String ancestor : this.ancestors) {
        // Is it expressed ?
        if (this.indexByColumn.containsKey(ancestor)) {
          int index = this.indexByColumn.get(ancestor);
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
