package io.squashql.query;

import io.squashql.query.compiled.CompiledComparisonMeasure;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.database.SqlUtils;
import io.squashql.type.TypedField;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;

public class ParentComparisonExecutor extends AComparisonExecutor {

  final CompiledComparisonMeasure pcm;

  public ParentComparisonExecutor(CompiledComparisonMeasure pcm) {
    this.pcm = pcm;
  }

  @Override
  protected BiPredicate<Object[], Header[]> createShiftProcedure(CompiledComparisonMeasure cm, ObjectIntMap<String> indexByColumn) {
    List<TypedField> ancestors = new ArrayList<>(this.pcm.ancestors());
    Collections.reverse(ancestors);
    return new ShiftProcedure(ancestors, indexByColumn);
  }

  static class ShiftProcedure implements BiPredicate<Object[], Header[]> {

    final List<TypedField> ancestors;
    final ObjectIntMap<String> indexByColumn;

    ShiftProcedure(List<TypedField> ancestors, ObjectIntMap<String> indexByColumn) {
      this.ancestors = ancestors;
      this.indexByColumn = indexByColumn;
    }

    @Override
    public boolean test(Object[] row, Header[] headers) {
      for (TypedField ancestor : this.ancestors) {
        // Is it expressed ?
        String name = SqlUtils.squashqlExpression(ancestor);
        if (this.indexByColumn.containsKey(name)) {
          int index = this.indexByColumn.get(name);
          if (!SQLTranslator.TOTAL_CELL.equals(row[index])) {
            row[index] = SQLTranslator.TOTAL_CELL;
            break;
          }
        }
      }
      return true;
    }
  }
}
