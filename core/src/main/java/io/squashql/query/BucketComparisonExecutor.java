package io.squashql.query;

import io.squashql.query.compiled.CompiledBucketColumnSet;
import io.squashql.query.compiled.CompiledComparisonMeasure;
import io.squashql.query.database.SqlUtils;
import io.squashql.type.NamedTypedField;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;

public class BucketComparisonExecutor extends AComparisonExecutor {

  final CompiledBucketColumnSet cSet;

  public BucketComparisonExecutor(CompiledBucketColumnSet cSet) {
    this.cSet = cSet;
  }

  @Override
  protected BiPredicate<Object[], Header[]> createShiftProcedure(CompiledComparisonMeasure cm, ObjectIntMap<String> indexByColumn) {
    return new ShiftProcedure(this.cSet, cm.referencePosition(), indexByColumn);
  }

  static class ShiftProcedure implements BiPredicate<Object[], Header[]> {

    final List<Pair<String, Object>> transformationByColumn;
    final ObjectIntMap<String> indexByColumn;
    final Map<String, List<String>> valuesByBucket = new LinkedHashMap<>();

    ShiftProcedure(CompiledBucketColumnSet cSet, Map<NamedTypedField, String> referencePosition, ObjectIntMap<String> indexByColumn) {
      this.valuesByBucket.putAll(cSet.values());
      this.indexByColumn = indexByColumn;
      this.transformationByColumn = new ArrayList<>();
      // Order does matter here
      this.transformationByColumn.add(Tuples.pair(SqlUtils.squashqlExpression(cSet.newField()), parse(referencePosition.get(cSet.newField()))));
      this.transformationByColumn.add(Tuples.pair(SqlUtils.squashqlExpression(cSet.field()), parse(referencePosition.get(cSet.field()))));
    }

    @Override
    public boolean test(Object[] row, Header[] headers) {
      Object bucketTransformation = this.transformationByColumn.get(0).getTwo();
      int bucketIndex = this.indexByColumn.getIfAbsent(this.transformationByColumn.get(0).getOne(), -1);
      if (bucketTransformation != null) {
        throw new RuntimeException("comparison with a different bucket value is not yet supported");
      }

      Object fieldTransformation = this.transformationByColumn.get(1).getTwo();
      if (fieldTransformation != null) {
        int fieldIndex = this.indexByColumn.getIfAbsent(this.transformationByColumn.get(1).getOne(), -1);
        String b = (String) row[bucketIndex];
        List<String> values = this.valuesByBucket.get(b);
        if (fieldTransformation instanceof Integer) {
          String fieldValue = (String) row[fieldIndex];
          int index = values.indexOf(fieldValue);
          row[fieldIndex] = values.get(Math.max(index + (int) fieldTransformation, 0));
        } else if (fieldTransformation.equals(REF_POS_FIRST)) {
          row[fieldIndex] = values.get(0);
        } else {
          throw new RuntimeException("not supported");
        }
      }
      return true;
    }
  }
}
