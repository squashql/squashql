package io.squashql.query;

import io.squashql.query.comp.BinaryOperations;
import io.squashql.store.Field;
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
    int readFromTableHeaderSize = readFromTable.headers().size();
    for (int index=0; index<readFromTableHeaderSize; index++) {
      Header readFromTableHeader = readFromTable.headers().get(index);
      if (!readFromTableHeader.isMeasure()) {
        indexByColumn.put(readFromTableHeader.field().name(), index);
      }
    }
    BiPredicate<Object[], Field[]> procedure = createShiftProcedure(cm, indexByColumn);

    int readFromTableColumnsCount = readFromTable.headers().stream().filter(header -> !header.isMeasure())
            .mapToInt(e -> 1).sum();
    Object[] buffer = new Object[readFromTableColumnsCount];
    Field[] fields = new Field[readFromTableColumnsCount];
    List<Object> result = new ArrayList<>((int) writeToTable.count());
    List<Object> readAggregateValues = readFromTable.getAggregateValues(cm.measure);
    List<Object> writeAggregateValues = writeToTable.getAggregateValues(cm.measure);
    BiFunction<Number, Number, Number> comparisonBiFunction = BinaryOperations.createComparisonBiFunction(cm.comparisonMethod, readFromTable.getField(cm.measure).type());
    int[] rowIndex = new int[1];
    IntIntMap mapping = buildMapping(writeToTable, readFromTable); // columns might be in a different order
    writeToTable.forEach(row -> {
      int i = 0;
      for (int columnIndex=0; columnIndex < readFromTableHeaderSize; columnIndex++) {
        Header header = readFromTable.headers().get(columnIndex);
        if (!header.isMeasure()) {
          fields[i] = header.field();
          int index = mapping.getIfAbsent(columnIndex, -1);
          buffer[i] = row.get(index);
          i++;
        }
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
    for (int index=0; index < readFromTable.headers().size(); index++) {
      Header header = readFromTable.headers().get(index);
      if (header.isMeasure()) {
        int writeToTableIndex = writeToTable.index(header.field());
        mapping.put(index, writeToTableIndex);
      }
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
