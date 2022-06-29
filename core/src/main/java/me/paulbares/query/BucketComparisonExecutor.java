package me.paulbares.query;

import me.paulbares.query.dto.BucketColumnSetDto;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

public class BucketComparisonExecutor extends AComparisonExecutor {

  final BucketColumnSetDto cSet;

  public BucketComparisonExecutor(BucketColumnSetDto cSet) {
    this.cSet = cSet;
  }

  @Override
  protected Predicate<Object[]> createShiftProcedure(BinaryOperationMeasure bom, ObjectIntMap<String> indexByColumn) {
    return new ShiftProcedure(this.cSet, bom.referencePosition, indexByColumn);
  }

  static class ShiftProcedure implements Predicate<Object[]> {

    final List<Pair<String, Object>> transformationByColumn;
    final ObjectIntMap<String> indexByColumn;

    final Map<String, List<String>> valuesByBucket = new LinkedHashMap<>();

    ShiftProcedure(BucketColumnSetDto cSet, Map<String, String> referencePosition, ObjectIntMap<String> indexByColumn) {
      for (Pair<String, List<String>> value : cSet.values) {
        this.valuesByBucket.put(value.getOne(), value.getTwo());
      }
      this.indexByColumn = indexByColumn;
      this.transformationByColumn = new ArrayList<>();
      // Order does matter here
      this.transformationByColumn.add(Tuples.pair(cSet.name, parse(referencePosition.get(cSet.name))));
      this.transformationByColumn.add(Tuples.pair(cSet.field, parse(referencePosition.get(cSet.field))));
    }

    @Override
    public boolean test(Object[] row) {
      Object bucketTransformation = this.transformationByColumn.get(0).getTwo();
      int bucketIndex = this.indexByColumn.get(this.transformationByColumn.get(0).getOne());
      if (bucketTransformation != null) {
        String bucket = (String) row[bucketIndex];
        row[bucketIndex] = null; // apply transformation
      }

      Object fieldTransformation = this.transformationByColumn.get(1).getTwo();
      if (fieldTransformation != null) {
        int fieldIndex = this.indexByColumn.get(this.transformationByColumn.get(1).getOne());
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
