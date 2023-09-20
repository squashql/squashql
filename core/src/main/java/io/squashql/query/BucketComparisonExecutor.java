package io.squashql.query;

import io.squashql.query.dto.BucketColumnSetDto;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiPredicate;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

public class BucketComparisonExecutor extends AComparisonExecutor {

  final BucketColumnSetDto cSet;

  public BucketComparisonExecutor(BucketColumnSetDto cSet) {
    this.cSet = cSet;
  }

  @Override
  protected BiPredicate<Object[], Header[]> createShiftProcedure(ComparisonMeasureReferencePosition cm, ObjectIntMap<String> indexByColumn) {
    return new ShiftProcedure(this.cSet, cm.referencePosition, indexByColumn);
  }

  static class ShiftProcedure implements BiPredicate<Object[], Header[]> {

    final List<Pair<String, Object>> transformationByColumn;
    final ObjectIntMap<String> indexByColumn;
    final Map<String, List<String>> valuesByBucket = new LinkedHashMap<>();

    ShiftProcedure(BucketColumnSetDto cSet, Map<Field, String> referencePosition, ObjectIntMap<String> indexByColumn) {
      this.valuesByBucket.putAll(cSet.values);
      this.indexByColumn = indexByColumn;
      this.transformationByColumn = new ArrayList<>();
      // Order does matter here
      this.transformationByColumn.add(Tuples.pair(cSet.name.name(), parse(referencePosition.get(cSet.name))));
      this.transformationByColumn.add(Tuples.pair(cSet.field.name(), parse(referencePosition.get(cSet.field))));
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
