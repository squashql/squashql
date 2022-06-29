package me.paulbares.query;

import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.query.dto.BucketColumnSetDto;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.api.tuple.Pair;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;
import org.eclipse.collections.impl.tuple.Tuples;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class BucketComparisonExecutor {

  public static List<Object> compare(
          BinaryOperationMeasure bom,
          BucketColumnSetDto columnSet,
          Table intermediateResult) {
    MutableObjectIntMap<String> indexByColumn = new ObjectIntHashMap<>();
    bom.referencePosition.entrySet().forEach(entry -> {
      int columnIndex = intermediateResult.columnIndex(entry.getKey());
      int index = Arrays.binarySearch(intermediateResult.columnIndices(), columnIndex);
      indexByColumn.put(entry.getKey(), index);
    });
    ShiftProcedure procedure = new ShiftProcedure(columnSet, bom.referencePosition, indexByColumn);

    Object[] buffer = new Object[intermediateResult.columnIndices().length];
    List<Object> result = new ArrayList<>((int) intermediateResult.count());
    int[] rowIndex = new int[1];
    List<Object> aggregateValues = intermediateResult.getAggregateValues(bom.measure);
    intermediateResult.forEach(row -> {
      int i = 0;
      for (int columnIndex : intermediateResult.columnIndices()) {
        buffer[i++] = row.get(columnIndex);
      }
      boolean success = procedure.execute(buffer);
      int position = intermediateResult.pointDictionary().getPosition(buffer);
      if (success && position != -1) {
        Object currentValue = aggregateValues.get(rowIndex[0]);
        Object referenceValue = aggregateValues.get(position);
        Object diff = BinaryOperations.compare(bom.method, currentValue, referenceValue, intermediateResult.getField(bom.measure).type());
        result.add(diff);
      } else {
        result.add(null); // nothing to compare with
      }
      rowIndex[0]++;
    });

    return result;
  }

  static class ShiftProcedure {

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

    private Object parse(String transformation) {
      Pattern shiftPattern = Pattern.compile("[a-zA-Z]+([-+])(\\d)");
      Pattern constantPattern = Pattern.compile("[a-zA-Z]+");
      Matcher m;
      if ((m = shiftPattern.matcher(transformation)).matches()) {
        String signum = m.group(1);
        String shift = m.group(2);
        return (signum.equals("-") ? -1 : 1) * Integer.valueOf(shift);
      } else if (constantPattern.matcher(transformation).matches()) {
        return null; // nothing to do
      } else {
        throw new RuntimeException("Unsupported transformation: " + transformation);
      }
    }

    public boolean execute(Object[] row) {
      Object bucketTransformation = this.transformationByColumn.get(0).getTwo();
      int bucketIndex = this.indexByColumn.get(this.transformationByColumn.get(0).getOne());
      if (bucketTransformation != null) {
        String bucket = (String) row[bucketIndex];
        row[bucketIndex] = null; // apply transformation
      }

      Object fieldTransformation = this.transformationByColumn.get(1).getTwo();
      if (fieldTransformation != null) {
        // apply
        int fieldIndex = this.indexByColumn.get(this.transformationByColumn.get(1).getOne());

        if (fieldTransformation instanceof Integer) {
          String b = (String) row[bucketIndex];
          String fieldValue = (String) row[fieldIndex];
          List<String> values = this.valuesByBucket.get(b);
          int index = values.indexOf(fieldValue);
          row[fieldIndex] = values.get(Math.max(index + (int) fieldTransformation, 0));
        } else {
          throw new RuntimeException("not supported");
        }
      }
      return true;
    }
  }
}
