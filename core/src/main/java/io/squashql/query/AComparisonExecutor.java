package io.squashql.query;

import io.squashql.query.comp.BinaryOperations;
import io.squashql.query.compiled.CompiledComparisonMeasure;
import io.squashql.table.Table;
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

public abstract class AComparisonExecutor<T extends CompiledComparisonMeasure> {

  public static final String REF_POS_FIRST = "first";

  protected abstract BiPredicate<Object[], Header[]> createShiftProcedure(T cm,
                                                                          ObjectIntMap<String> indexByColumn,
                                                                          Table readFromTable);

  public List<Object> compare(
          T cm,
          Table writeToTable,
          Table readFromTable) {
    MutableObjectIntMap<String> indexByColumn = new ObjectIntHashMap<>();
    int readFromTableHeaderSize = readFromTable.headers().size();
    int index = 0;
    for (Header header : readFromTable.headers()) {
      if (!header.isMeasure()) {
        indexByColumn.put(header.name(), index++);
      }
    }
    BiPredicate<Object[], Header[]> procedure = createShiftProcedure(cm, indexByColumn, readFromTable);

    int readFromTableColumnsCount = (int) readFromTable.headers().stream().filter(header -> !header.isMeasure()).count();
    Object[] buffer = new Object[readFromTableColumnsCount];
    Header[] headers = new Header[readFromTableColumnsCount];
    List<Object> result = new ArrayList<>(writeToTable.count());
    List<Object> readAggregateValues = readFromTable.getAggregateValues(cm.measure());
    List<Object> writeAggregateValues = writeToTable.getAggregateValues(cm.measure());
    BiFunction<Object, Object, Object> comparisonBiFunction = cm.comparisonMethod() != null
            ? (a, b) -> BinaryOperations.createComparisonBiFunction(cm.comparisonMethod(), readFromTable.getHeader(cm.measure()).type()).apply((Number) a, (Number) b)
            : cm.comparisonOperator();
    int[] rowIndex = new int[1];
    IntIntMap mapping = buildMapping(writeToTable, readFromTable); // columns might be in a different order
    writeToTable.forEach(row -> {
      int i = 0;
      for (int columnIndex = 0; columnIndex < readFromTableHeaderSize; columnIndex++) {
        Header header = readFromTable.headers().get(columnIndex);
        if (!header.isMeasure()) {
          headers[i] = header;
          buffer[i] = row.get(mapping.getIfAbsent(columnIndex, -1));
          i++;
        }
      }
      boolean success = procedure.test(buffer, headers);
      int readPosition = readFromTable.pointDictionary().getPosition(buffer);
      if (success && readPosition != -1) {
        Object currentValue = writeAggregateValues.get(rowIndex[0]);
        Object referenceValue = readAggregateValues.get(readPosition);
        Object diff = comparisonBiFunction.apply(currentValue, referenceValue);
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
    for (int index = 0; index < readFromTable.headers().size(); index++) {
      Header header = readFromTable.headers().get(index);
      if (!header.isMeasure()) {
        int writeToTableIndex = writeToTable.index(header);
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
