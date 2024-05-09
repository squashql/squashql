package io.squashql.query;

import io.squashql.query.compiled.CompiledComparisonMeasureReferencePosition;
import io.squashql.query.database.SqlTranslator;
import io.squashql.query.database.SqlUtils;
import io.squashql.table.Table;
import io.squashql.type.TypedField;
import io.squashql.util.CustomExplicitOrdering;
import lombok.AllArgsConstructor;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

@AllArgsConstructor
public class SingleGroupComparisonExecutor extends AComparisonExecutor<CompiledComparisonMeasureReferencePosition> {

  private final TypedField field;

  @Override
  protected BiPredicate<Object[], Header[]> createShiftProcedure(CompiledComparisonMeasureReferencePosition cm,
                                                                 ObjectIntMap<String> indexByColumn,
                                                                 Table readFromTable) {
    return new ShiftProcedure(this.field, cm.referencePosition(), indexByColumn, cm.elements(), readFromTable);
  }

  private static class ShiftProcedure implements BiPredicate<Object[], Header[]> {

    private final Pair<String, Object> columnAndTransformation;
    private final ObjectIntMap<String> indexByColumn;
    private final List<Object> values;

    private ShiftProcedure(TypedField field, Map<TypedField, String> referencePosition, ObjectIntMap<String> indexByColumn, List<?> elements, Table readFromTable) {
      this.indexByColumn = indexByColumn;
      // Order does matter here
      String column = SqlUtils.squashqlExpression(field);
      this.columnAndTransformation = Tuples.pair(column, parse(referencePosition.get(field)));
      Object[] array = readFromTable.getColumnValues(column).stream().filter(o -> !SqlTranslator.TOTAL_CELL.equals(o)).toArray();
      Arrays.sort(array, elements == null ? null : new CustomExplicitOrdering(elements));
      this.values = Arrays.asList(array);
    }

    @Override
    public boolean test(Object[] row, Header[] headers) {
      Object fieldTransformation = this.columnAndTransformation.getTwo();
      int fieldIndex = this.indexByColumn.getIfAbsent(this.columnAndTransformation.getOne(), -1);
      return GroupComparisonExecutor.GroupComparisonShiftProcedure.shift(fieldIndex, fieldTransformation, this.values, row);
    }
  }
}
