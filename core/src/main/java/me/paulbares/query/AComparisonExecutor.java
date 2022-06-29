package me.paulbares.query;

import me.paulbares.query.comp.BinaryOperations;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Predicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AComparisonExecutor {

  public static final String REF_POS_FIRST = "first";

  protected abstract Predicate<Object[]> createShiftProcedure(BinaryOperationMeasure bom, ObjectIntMap<String> indexByColumn);

  public List<Object> compare(
          BinaryOperationMeasure bom,
          Table intermediateResult) {
    MutableObjectIntMap<String> indexByColumn = new ObjectIntHashMap<>();
    bom.referencePosition.entrySet().forEach(entry -> {
      int columnIndex = intermediateResult.columnIndex(entry.getKey());
      int index = Arrays.binarySearch(intermediateResult.columnIndices(), columnIndex);
      indexByColumn.put(entry.getKey(), index);
    });
    Predicate<Object[]> procedure = createShiftProcedure(bom, indexByColumn);

    Object[] buffer = new Object[intermediateResult.columnIndices().length];
    List<Object> result = new ArrayList<>((int) intermediateResult.count());
    int[] rowIndex = new int[1];
    List<Object> aggregateValues = intermediateResult.getAggregateValues(bom.measure);
    intermediateResult.forEach(row -> {
      int i = 0;
      for (int columnIndex : intermediateResult.columnIndices()) {
        buffer[i++] = row.get(columnIndex);
      }
      boolean success = procedure.test(buffer);
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

  public static Object parse(String transformation) {
    if (transformation.equals(REF_POS_FIRST)) {
      return REF_POS_FIRST;
    }
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
}
