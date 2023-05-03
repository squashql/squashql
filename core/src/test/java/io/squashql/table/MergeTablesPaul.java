package io.squashql.table;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.squashql.query.ColumnarTable;
import io.squashql.query.Header;
import io.squashql.query.Measure;
import io.squashql.query.Table;
import io.squashql.query.database.SQLTranslator;
import io.squashql.query.dto.JoinType;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.list.mutable.primitive.MutableIntListFactoryImpl;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;
import org.eclipse.collections.impl.set.mutable.primitive.MutableIntSetFactoryImpl;

import java.util.*;
import java.util.stream.IntStream;

class MergeTablesPaul {

  private MergeTablesPaul() {
  }

  /**
   * Same as {@link #mergeTables(Table, Table, JoinType)} but with {@code JoinType = FULL}
   */
  public static Table mergeTables(Table leftTable, Table rightTable) {
    return mergeTables(leftTable, rightTable, JoinType.FULL);
  }

  /**
   * Merge two tables into only one resulting table. We choose to first get all columns and then all measures.
   * Hypothesis:
   * <ul>
   *   <li>the input tables must have their common columns at the beginning, in the same order, and sorted</li>
   *   <li>every table must have all its columns first, then its measures</li>
   *   <li>the input tables must not share any measure</li>
   * </ul>
   */
  public static Table mergeTables(Table leftTable, Table rightTable, JoinType joinType) {
    final Holder mergedTableHeaders = mergeHeaders(leftTable, rightTable);
    final Set<Measure> mergedTableMeasures = mergeMeasures(leftTable.measures(), rightTable.measures());
    final List<List<Object>> mergedValues = mergeValues(mergedTableHeaders, leftTable, rightTable);

    return new ColumnarTable(
            mergedTableHeaders.headers,
            mergedTableMeasures,
            mergedValues);
  }

  private static Holder mergeHeaders(Table leftTable, Table rightTable) {
    List<Header> mergedColumns = new ArrayList<>();
    List<Header> mergedMeasures = new ArrayList<>();
    MutableIntList leftMappingList = MutableIntListFactoryImpl.INSTANCE.empty();
    MutableIntList rightMappingList = MutableIntListFactoryImpl.INSTANCE.empty();
    leftTable.headers().forEach(leftHeader -> {
      if (leftHeader.isMeasure()) {
        mergedMeasures.add(leftHeader);
      } else {
        mergedColumns.add(leftHeader);
        leftMappingList.add(mergedColumns.indexOf(leftHeader));
      }
    });
    rightTable.headers().forEach(rightHeader -> {
      if (rightHeader.isMeasure()) {
        if (mergedMeasures.contains(rightHeader)) {
          throw new UnsupportedOperationException(String.format(
                  "The two tables both contain the measure %s while they must not share any measure to be merged.",
                  rightHeader.name()));
        } else {
          mergedMeasures.add(rightHeader);
        }
      } else {
        if (!mergedColumns.contains(rightHeader)) {
          mergedColumns.add(rightHeader);
        }
        rightMappingList.add(mergedColumns.indexOf(rightHeader));
      }
    });

    List<Header> mergedTableHeaders = new ArrayList<>(mergedColumns);
    mergedTableHeaders.addAll(mergedMeasures);
    return new Holder(mergedTableHeaders, leftMappingList, rightMappingList);
  }

  private static Set<Measure> mergeMeasures(Set<Measure> leftMeasures, Set<Measure> rightMeasures) {
    return Sets.newHashSet(Iterables.concat(leftMeasures, rightMeasures));
  }

  private static List<List<Object>> mergeValues(
          Holder holder,
          Table leftTable,
          Table rightTable) {
    List<Header> mergedTableHeaders = holder.headers;
    Object[] mergedBuffer = new Object[(int) mergedTableHeaders.stream().filter(h -> !h.isMeasure()).count()];
    Object[] leftBuffer = new Object[(int) leftTable.headers().stream().filter(h -> !h.isMeasure()).count()];
    Object[] rightBuffer = new Object[(int) rightTable.headers().stream().filter(h -> !h.isMeasure()).count()];

    int[] leftMapping = holder.leftMapping;
    int[] rightMapping = holder.rightMapping;

    long leftCountMeasure = leftTable.headers().stream().filter(Header::isMeasure).count();
    long rightCountMeasure = rightTable.headers().stream().filter(Header::isMeasure).count();
    List<ArrayList<Object>> measureValues = IntStream.range(0, (int) (leftCountMeasure + rightCountMeasure))
            .mapToObj(i -> new ArrayList<>()).toList();
    List<ArrayList<Object>> pointValues = IntStream.range(0, mergedBuffer.length)
            .mapToObj(i -> new ArrayList<>()).toList();

    BitSet alreadyVisited = new BitSet();

    List<Measure> leftMeasures = getMeasures((ColumnarTable) leftTable);
    List<Measure> rightMeasures = getMeasures((ColumnarTable) rightTable);
    final int[] position = {-1};

    leftTable.pointDictionary().forEach((point, row) -> {
      Arrays.fill(mergedBuffer, SQLTranslator.TOTAL_CELL);
      for (int i = 0; i < point.length; i++) {
        mergedBuffer[leftMapping[i]] = point[i];
      }

      for (int i = 0; i < rightMapping.length; i++) {
        rightBuffer[i] = mergedBuffer[rightMapping[i]];
      }

      for (int i = 0; i < mergedBuffer.length; i++) {
        pointValues.get(i).add(mergedBuffer[i]);
      }

      for (int i = 0; i < leftCountMeasure; i++) {
        measureValues.get(i).add(leftTable.getAggregateValues(leftMeasures.get(i)).get(row));
      }

      int[] complement = complement(holder.leftMapping, holder.rightMapping);
      boolean missingColumnsAreTotal = true;
      for (int c : complement) {
        missingColumnsAreTotal &= SQLTranslator.TOTAL_CELL.equals(mergedBuffer[c]);
      }

      boolean condition = (holder.leftIsIncludeInRight || missingColumnsAreTotal) && (position[0] = rightTable.pointDictionary().getPosition(rightBuffer)) >= 0;
      for (int i = 0; i < rightCountMeasure; i++) {
        Object value = null;
        if (condition) {
          value = rightTable.getAggregateValues(rightMeasures.get(i)).get(position[0]);
        }
        measureValues.get((int) (i + leftCountMeasure)).add(value);
      }

      if (condition) {
        alreadyVisited.set(position[0]);
      }
    });

    rightTable.pointDictionary().forEach((point, row) -> {
      if (alreadyVisited.get(row)) {
        return;
      }
      Arrays.fill(mergedBuffer, SQLTranslator.TOTAL_CELL);
      for (int i = 0; i < point.length; i++) {
        mergedBuffer[rightMapping[i]] = point[i];
      }

      for (int i = 0; i < leftMapping.length; i++) {
        leftBuffer[i] = mergedBuffer[leftMapping[i]];
      }

      for (int i = 0; i < mergedBuffer.length; i++) {
        pointValues.get(i).add(mergedBuffer[i]);
      }

      for (int i = 0; i < leftCountMeasure; i++) {
        measureValues.get(i).add(null);
      }

      for (int i = 0; i < rightCountMeasure; i++) {
        measureValues.get((int) (i + leftCountMeasure)).add(rightTable.getAggregateValues(rightMeasures.get(i)).get(row));
      }
    });

    List<List<Object>> result = new ArrayList<>(pointValues);
    result.addAll(measureValues);
    return result;
  }

  private static List<Measure> getMeasures(ColumnarTable table) {
    return table.headers().stream()
            .filter(Header::isMeasure)
            .map(h -> table.measures().stream().filter(m -> m.alias().equals(h.name())).findFirst().orElseThrow(() -> new IllegalStateException("Cannot find measure with name " + h.name())))
            .toList();
  }

  static class Holder {

    final List<Header> headers;
    final int[] leftMapping;
    final int[] rightMapping;
    final boolean leftIsIncludeInRight;

    public Holder(List<Header> headers, IntList leftMapping, IntList rightMapping) {
      this.headers = headers;
      this.leftMapping = leftMapping.toArray();
      this.rightMapping = rightMapping.toArray();
      this.leftIsIncludeInRight = isLeftListIncludeInRightList(leftMapping, rightMapping);
    }
  }

  private static boolean isLeftListIncludeInRightList(IntList left, IntList right) {
    MutableIntSet s = new IntHashSet(left);
    s.removeAll(right);
    return s.isEmpty();
  }

  public static int[] complement(int[] a, int[] b) {
    MutableIntSet s1 = MutableIntSetFactoryImpl.INSTANCE.of(a);
    MutableIntSet s2 = MutableIntSetFactoryImpl.INSTANCE.of(b);
    s1.removeAll(s2);
    return s1.toArray();
  }
}
