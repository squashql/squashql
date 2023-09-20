package io.squashql.table;

import io.squashql.query.Field;
import io.squashql.query.Header;
import io.squashql.query.Measure;
import io.squashql.query.TotalCountMeasure;
import io.squashql.query.dictionary.ObjectArrayDictionary;
import io.squashql.query.dto.QueryDto;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.eclipse.collections.api.list.primitive.IntList;
import org.eclipse.collections.api.list.primitive.MutableIntList;
import org.eclipse.collections.impl.list.mutable.primitive.MutableIntListFactoryImpl;

public interface Table extends Iterable<List<Object>> {

  ObjectArrayDictionary pointDictionary();

  List<Header> headers();

  Set<Measure> measures();

  void addAggregates(Header header, Measure measure, List<Object> values);

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

  default List<Object> getAggregateValues(Measure measure) {
    int index = headers().indexOf(getHeader(measure));
    if (index < 0) {
      throw new IllegalArgumentException("no aggregate values for " + measure);
    }
    return getColumn(index);
  }

  default Header getHeader(Measure measure) {
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

  default IntList columnIndices(Field column) {
    int i = 0;
    MutableIntList list = MutableIntListFactoryImpl.INSTANCE.empty();
    for (Header header : headers()) {
      if (header.name().equals(column)) {
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
    return measures().contains(TotalCountMeasure.INSTANCE)
            ? (long) getAggregateValues(TotalCountMeasure.INSTANCE).get(0) : -1;
  }

  void show(int numRows);

  default void show() {
    show(Integer.MAX_VALUE);
  }
}
