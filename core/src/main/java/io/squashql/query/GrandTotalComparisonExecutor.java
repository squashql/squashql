package io.squashql.query;

import io.squashql.query.compiled.CompiledGrandTotalComparisonMeasure;
import io.squashql.query.database.SQLTranslator;
import io.squashql.table.Table;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;

import java.util.function.BiPredicate;

public class GrandTotalComparisonExecutor extends AComparisonExecutor<CompiledGrandTotalComparisonMeasure> {

  @Override
  protected BiPredicate<Object[], Header[]> createShiftProcedure(CompiledGrandTotalComparisonMeasure cm,
                                                                 ObjectIntMap<String> indexByColumn,
                                                                 Table readFromTable) {
    return (row, __) -> {
      indexByColumn.forEachKeyValue((value, index) -> row[index] = SQLTranslator.TOTAL_CELL);
      return true;
    };
  }
}
