package me.paulbares.query;

import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.store.Field;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AComparisonExecutor {

  public static final String REF_POS_FIRST = "first";

  protected abstract BiPredicate<Object[], Field[]> createShiftProcedure(ComparisonMeasureReferencePosition cm, ObjectIntMap<String> indexByColumn);

  public abstract ColumnSet getColumnSet();

  public List<Object> compare(
          ComparisonMeasureReferencePosition cm,
          Table intermediateResult) {
    MutableObjectIntMap<String> indexByColumn = new ObjectIntHashMap<>();
    cm.referencePosition.entrySet().forEach(entry -> {
      int columnIndex = intermediateResult.columnIndex(entry.getKey());
      int index = Arrays.binarySearch(intermediateResult.columnIndices(), columnIndex);
      indexByColumn.put(entry.getKey(), index);
    });
    BiPredicate<Object[], Field[]> procedure = createShiftProcedure(cm, indexByColumn);

    Object[] buffer = new Object[intermediateResult.columnIndices().length];
    Field[] fields = new Field[intermediateResult.columnIndices().length];
    List<Object> result = new ArrayList<>((int) intermediateResult.count());
    int[] rowIndex = new int[1];
    List<Object> aggregateValues = intermediateResult.getAggregateValues(cm.measure);
    BiFunction<Number, Number, Number> comparisonBiFunction = BinaryOperations.createComparisonBiFunction(cm.method, intermediateResult.getField(cm.measure).type());
    intermediateResult.forEach(row -> {
      int i = 0;
      for (int columnIndex : intermediateResult.columnIndices()) {
        buffer[i] = row.get(columnIndex);
        fields[i] = intermediateResult.headers().get(i);
        i++;
      }
      boolean success = procedure.test(buffer, fields);
      int position = intermediateResult.pointDictionary().getPosition(buffer);
      if (success && position != -1) {
        Object currentValue = aggregateValues.get(rowIndex[0]);
        Object referenceValue = aggregateValues.get(position);
        Object diff = comparisonBiFunction.apply((Number) currentValue, (Number) referenceValue);
        result.add(diff);
      } else {
        result.add(null); // nothing to compare with
      }
      rowIndex[0]++;
    });

    return result;
  }

  public static Object parse(String transformation) {
    if (transformation == null) {
      return null;
    }

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
