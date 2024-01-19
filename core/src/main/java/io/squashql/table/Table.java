package io.squashql.table;

import io.squashql.query.Header;
import io.squashql.query.NamedField;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.dictionary.ObjectArrayDictionary;
import io.squashql.query.dto.QueryDto;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.list.mutable.primitive.MutableIntListFactoryImpl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static io.squashql.query.compiled.CompiledExpressionMeasure.COMPILED_TOTAL_COUNT;

public interface Table extends Iterable<List<Object>> {

  ObjectArrayDictionary pointDictionary();

  List<Header> headers();

  Set<CompiledMeasure> measures();

  /**
   * Adds the given aggregates values corresponding to this measure to the table (adds a new column). The order of the
   * aggregates is expected to match the order of the rows in this table. If the order is not known, it is better to use
   * {@link #transferAggregates(Table, CompiledMeasure)}.
   */
  void addAggregates(Header header, CompiledMeasure measure, List<Object> values);

  void transferAggregates(Table from, CompiledMeasure measure);

  default List<Object> getColumn(int columnIndex) {
    List<Object> elements = new ArrayList<>();
    for (List<Object> objects : this) {
      elements.add(objects.get(columnIndex));
    }
    return elements;
  }

  default List<Object> getColumnValues(String column) {
    return getColumn(columnIndex(column));
  }

  default List<Object> getAggregateValues(CompiledMeasure measure) {
    int index = headers().indexOf(getHeader(measure));
    if (index < 0) {
      throw new IllegalArgumentException("no aggregate values for " + measure);
    }
    return getColumn(index);
  }

  default Header getHeader(CompiledMeasure measure) {
    return headers().stream().filter(header -> header.name().equals(measure.alias()))
            .findAny().orElseThrow(() -> new IllegalArgumentException("no header for " + measure));
  }

  default Header getHeader(String column) {
    return headers().get(columnIndex(column));
  }

  default int columnIndex(String column) {
    int index = -1, i = 0;
    for (Header header : headers()) {
      if (header.name().equals(column)) {
        index = i;
        break;
      }
      i++;
    }
    if (index < 0) {
      throw new IllegalArgumentException("no column named " + column + ". Available columns are " + headers().stream().map(Header::name).toList());
    }
    return index;
  }

  default IntList columnIndices(NamedField column) {
    int i = 0;
    MutableIntList list = MutableIntListFactoryImpl.INSTANCE.empty();
    for (Header header : headers()) {
      if (header.name().equals(column.name())) {
        list.add(i);
      }
      i++;
    }
    return list;
  }

  default int index(Header header) {
    int index = headers().indexOf(header);
    if (index < 0) {
      throw new IllegalArgumentException("no header named " + header);
    }
    return index;
  }

  /**
   * Returns the number of rows in the table.
   *
   * @return the number of rows
   */
  long count();

  /**
   * Returns the total number of rows before applying the query limit as in {@link QueryDto#limit}.
   */
  default long totalCount() {
    return measures().contains(COMPILED_TOTAL_COUNT)
            ? (long) getAggregateValues(COMPILED_TOTAL_COUNT).get(0)
            : -1;
  }

  default void show(int numRows) {
    System.out.println(toString(numRows));
  }

  default void show() {
    show(Integer.MAX_VALUE);
  }

  default String toString(int numRows) {
    return TableUtils.toString(headers(), () -> new Iterator<>() {

      Iterator<List<Object>> underlying = iterator();
      int[] c = new int[1];

      @Override
      public boolean hasNext() {
        return this.c[0] < numRows ? this.underlying.hasNext() : false;
      }

      @Override
      public List<Object> next() {
        this.c[0]++;
        return this.underlying.next();
      }
    }, h -> ((Header) h).name(), String::valueOf);
  }
}
