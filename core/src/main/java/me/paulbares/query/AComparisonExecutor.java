package me.paulbares.query;

import me.paulbares.query.comp.BinaryOperations;
import me.paulbares.store.Field;
import me.paulbares.util.AitmArrays;
import org.eclipse.collections.api.map.primitive.IntIntMap;
import org.eclipse.collections.api.map.primitive.MutableIntIntMap;
import org.eclipse.collections.api.map.primitive.MutableObjectIntMap;
import org.eclipse.collections.api.map.primitive.ObjectIntMap;
import org.eclipse.collections.impl.map.mutable.primitive.IntIntHashMap;
import org.eclipse.collections.impl.map.mutable.primitive.ObjectIntHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class AComparisonExecutor {

  public static final String REF_POS_FIRST = "first";

  protected abstract BiPredicate<Object[], Field[]> createShiftProcedure(ComparisonMeasureReferencePosition cm, ObjectIntMap<String> indexByColumn);

  public List<Object> compare(
          ComparisonMeasureReferencePosition cm,
          Table writeToTable,
          Table readFromTable) {
    MutableObjectIntMap<String> indexByColumn = new ObjectIntHashMap<>();
    for (int columnIndex : readFromTable.columnIndices()) {
      int index = AitmArrays.search(readFromTable.columnIndices(), columnIndex);
      indexByColumn.put(readFromTable.headers().get(columnIndex).name(), index);
    }
    BiPredicate<Object[], Field[]> procedure = createShiftProcedure(cm, indexByColumn);

    Object[] buffer = new Object[readFromTable.columnIndices().length];
    Field[] fields = new Field[readFromTable.columnIndices().length];
    List<Object> result = new ArrayList<>((int) writeToTable.count());
    List<Object> readAggregateValues = readFromTable.getAggregateValues(cm.measure);
    List<Object> writeAggregateValues = writeToTable.getAggregateValues(cm.measure);
    BiFunction<Number, Number, Number> comparisonBiFunction = BinaryOperations.createComparisonBiFunction(cm.comparisonMethod, readFromTable.getField(cm.measure).type());
    int[] rowIndex = new int[1];
    IntIntMap mapping = buildMapping(writeToTable, readFromTable); // columns might be in a different order
    writeToTable.forEach(row -> {
      int i = 0;
      for (int columnIndex : readFromTable.columnIndices()) {
        fields[i] = readFromTable.headers().get(columnIndex);
        int index = mapping.getIfAbsent(columnIndex, -1);
        buffer[i] = row.get(index);
        i++;
      }
      boolean success = procedure.test(buffer, fields);
      int readPosition = readFromTable.pointDictionary().getPosition(buffer);
      if (success && readPosition != -1) {
        Object currentValue = writeAggregateValues.get(rowIndex[0]);
        Object referenceValue = readAggregateValues.get(readPosition);
        Object diff = comparisonBiFunction.apply((Number) currentValue, (Number) referenceValue);
        result.add(diff);
      } else {
        result.add(null); // nothing to compare with
      }
      rowIndex[0]++;
    });

    return result;
  }

  public IntIntMap buildMapping(Table writeToTable, Table readFromTable) {
    MutableIntIntMap mapping = new IntIntHashMap();
    for (int columnIndex : readFromTable.columnIndices()) {
      Field field = readFromTable.headers().get(columnIndex);
      int index = writeToTable.index(field);
      mapping.put(columnIndex, index);
    }
    return mapping;
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
