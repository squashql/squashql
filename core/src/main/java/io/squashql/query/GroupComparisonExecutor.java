package io.squashql.query;

import io.squashql.query.compiled.CompiledGroupColumnSet;
import io.squashql.query.compiled.CompiledComparisonMeasureReferencePosition;
import io.squashql.query.database.SqlUtils;
import io.squashql.table.Table;
import io.squashql.type.TypedField;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

public class GroupComparisonExecutor extends AComparisonExecutor<CompiledComparisonMeasureReferencePosition> {

  private final CompiledGroupColumnSet cSet;

  public GroupComparisonExecutor(CompiledGroupColumnSet cSet) {
    this.cSet = cSet;
  }

  @Override
  protected BiPredicate<Object[], Header[]> createShiftProcedure(CompiledComparisonMeasureReferencePosition cm,
                                                                 ObjectIntMap<String> indexByColumn,
                                                                 Table readFromTable) {
    return new GroupComparisonShiftProcedure(this.cSet, cm.referencePosition(), indexByColumn);
  }

  static class GroupComparisonShiftProcedure implements BiPredicate<Object[], Header[]> {

    private final List<Pair<String, Object>> columnAndTransformations;
    private final ObjectIntMap<String> indexByColumn;
    private final Map<Object, List<Object>> valuesByGroup = new LinkedHashMap<>();

    private GroupComparisonShiftProcedure(CompiledGroupColumnSet cSet, Map<TypedField, String> referencePosition, ObjectIntMap<String> indexByColumn) {
      this.valuesByGroup.putAll(cSet.values());
      this.indexByColumn = indexByColumn;
      this.columnAndTransformations = new ArrayList<>();
      // Order does matter here
      this.columnAndTransformations.add(Tuples.pair(SqlUtils.squashqlExpression(cSet.newField()), parse(referencePosition.get(cSet.newField()))));
      this.columnAndTransformations.add(Tuples.pair(SqlUtils.squashqlExpression(cSet.field()), parse(referencePosition.get(cSet.field()))));
    }

    @Override
    public boolean test(Object[] row, Header[] headers) {
      Object groupTransformation = this.columnAndTransformations.get(0).getTwo();
      int groupIndex = this.indexByColumn.getIfAbsent(this.columnAndTransformations.get(0).getOne(), -1);
      if (groupTransformation != null) {
        throw new RuntimeException("comparison with a different group value is not yet supported");
      }

      String b = (String) row[groupIndex];
      List<Object> values = this.valuesByGroup.get(b);
      String column = this.columnAndTransformations.get(1).getOne();
      Object fieldTransformation = this.columnAndTransformations.get(1).getTwo();
      int fieldIndex = this.indexByColumn.getIfAbsent(column, -1);
      return shift(fieldIndex, fieldTransformation, values, row);
    }

    static boolean shift(int fieldIndex, Object fieldTransformation, List<Object> values, Object[] row) {
      if (fieldTransformation instanceof Integer) {
        String fieldValue = (String) row[fieldIndex];
        int index = values.indexOf(fieldValue);
        if (index < 0) {
          return false; // not found. It is possible when fieldValue eq __total__ for instance
        }
        row[fieldIndex] = values.get(Math.max(index + (int) fieldTransformation, 0));
      } else if (fieldTransformation.equals(REF_POS_FIRST)) {
        row[fieldIndex] = values.get(0);
      } else {
        throw new RuntimeException("not supported");
      }
      return true;
    }
  }
}
