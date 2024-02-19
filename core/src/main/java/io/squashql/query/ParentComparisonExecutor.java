package io.squashql.query;

import io.squashql.query.compiled.CompiledComparisonMeasureReferencePosition;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.database.SqlUtils;
import io.squashql.type.TypedField;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.BiPredicate;

public class ParentComparisonExecutor extends AComparisonExecutor<CompiledComparisonMeasureReferencePosition> {

  private final CompiledComparisonMeasureReferencePosition pcm;

  public ParentComparisonExecutor(CompiledComparisonMeasureReferencePosition pcm) {
    this.pcm = pcm;
  }

  @Override
  protected BiPredicate<Object[], Header[]> createShiftProcedure(CompiledComparisonMeasureReferencePosition cm, ObjectIntMap<String> indexByColumn) {
    List<TypedField> ancestors = new ArrayList<>(this.pcm.ancestors());
    Collections.reverse(ancestors);
    return new AncestorShiftProcedure(ancestors, indexByColumn, cm.grandTotalAlongAncestors());
  }

  record AncestorShiftProcedure(List<TypedField> ancestors,
                                ObjectIntMap<String> indexByColumn,
                                boolean grandTotalAlongAncestors) implements BiPredicate<Object[], Header[]> {

    @Override
    public boolean test(Object[] row, Header[] headers) {
      for (TypedField ancestor : this.ancestors) {
        // Is it expressed ?
        String name = SqlUtils.squashqlExpression(ancestor);
        if (this.indexByColumn.containsKey(name)) {
          int index = this.indexByColumn.get(name);
          if (!SQLTranslator.TOTAL_CELL.equals(row[index])) {
            row[index] = SQLTranslator.TOTAL_CELL;
            if (!this.grandTotalAlongAncestors) {
              break;
            }
          }
        }
      }
      return true;
    }
  }
}
