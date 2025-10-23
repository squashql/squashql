package io.squashql.table;

import com.google.common.base.Suppliers;
import io.squashql.query.Header;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.dictionary.ObjectArrayDictionary;
import io.squashql.util.ListUtils;

import java.util.*;
import java.util.function.Supplier;

public class ColumnarTable implements Table {

  private final Supplier<ObjectArrayDictionary> pointDictionary;
  private final List<Header> headers;
  private final Set<CompiledMeasure> measures;
  private final List<List<Object>> values;

  public ColumnarTable(List<Header> headers, Set<CompiledMeasure> measures, List<List<Object>> values) {
    if (headers.stream().filter(Header::isMeasure)
            .anyMatch(measureHeader -> !measures.stream().map(CompiledMeasure::alias).toList()
                    .contains(measureHeader.name()))) {
      throw new IllegalArgumentException("Every header measure should have its description in measures.");
    }
    this.headers = new ArrayList<>(headers);
    this.measures = new HashSet<>(measures);
    this.values = new ArrayList<>(values);
    this.pointDictionary = Suppliers.memoize(() -> createPointDictionary(this));
  }

  public static ObjectArrayDictionary createPointDictionary(Table table) {
    int pointLength = (int) table.headers().stream().filter(header -> !header.isMeasure()).count();
    ObjectArrayDictionary dictionary = new ObjectArrayDictionary(pointLength);
    table.forEach(row -> {
      Object[] columnValues = new Object[pointLength];
      int i = 0;
      for (int index = 0; index < table.headers().size(); index++) {
        if (!table.headers().get(index).isMeasure()) {
          columnValues[i++] = row.get(index);
        }
      }
      dictionary.map(columnValues);
    });
    return dictionary;
  }

  @Override
  public void addAggregates(Header header, CompiledMeasure measure, List<Object> values) {
    this.headers.add(new Header(header.name(), header.type(), true));
    this.measures.add(measure);
    this.values.add(values);
  }

  /**
   * BE CAREFUL !! This method assumes this table and the table from passed in arguments have the same headers
   * {@code Header#isMeasure == false} in the same order.
   */
  @Override
  public void transferAggregates(Table from, CompiledMeasure measure) {
    if (this.headers.stream().filter(h -> !h.isMeasure()).count() !=
            from.headers().stream().filter(h -> !h.isMeasure()).count()) {
      List<String> toHeaderNames = this.headers.stream().filter(h -> !h.isMeasure()).map(Header::name).toList();
      List<String> fromHeaderNames = from.headers().stream().filter(h -> !h.isMeasure()).map(Header::name).toList();
      throw new IllegalArgumentException(
              "The aggregates you are trying to transfer comes from a table that has the following headers " + fromHeaderNames
                      + " but does not match the headers of the destination table " + toHeaderNames);
    }

    List<Object> values = ListUtils.createListWithNulls(count());
    List<Object> aggregateValues = from.getAggregateValues(measure);
    this.pointDictionary.get().forEach((point, index) -> {
      int position = from.pointDictionary().getPosition(point);
      if (position >= 0) {
        values.set(index, aggregateValues.get(position));
      }
    });
    Header header = from.getHeader(measure);
    this.headers.add(new Header(header.name(), header.type(), true));
    this.measures.add(measure);
    this.values.add(values);
  }

  @Override
  public List<Object> getColumn(int columnIndex) {
    return this.values.get(columnIndex);
  }

  public List<List<Object>> getColumns() {
    return this.values;
  }

  @Override
  public int count() {
    return this.values.get(0).size();
  }

  @Override
  public ObjectArrayDictionary pointDictionary() {
    return this.pointDictionary.get();
  }

  @Override
  public Set<CompiledMeasure> measures() {
    return this.measures;
  }

  @Override
  public List<Header> headers() {
    return this.headers;
  }

  @Override
  public String toString() {
    return toString(Integer.MAX_VALUE);
  }

  @Override
  public String toCSV() {
    return TableUtils.toCSV(
      this.headers.stream().map(header -> header.name()).toList(),
      TableUtils.transpose(this.values, count(), this.headers.size())
    );
  }

  @Override
  public Iterator<List<Object>> iterator() {
    return new ColumnarTableIterator();
  }

  public ColumnarTable copy() {
    List<List<Object>> newValues = new ArrayList<>();
    for (int i = 0; i < this.headers.size(); i++) {
      newValues.add(new ArrayList<>(getColumn(i)));
    }
    return new ColumnarTable(this.headers, this.measures, newValues);
  }

  private class ColumnarTableIterator implements Iterator<List<Object>> {

    int current = 0;
    int size = count();

    @Override
    public boolean hasNext() {
      return this.current != this.size;
    }

    @Override
    public List<Object> next() {
      if (this.current >= this.size) {
        throw new NoSuchElementException();
      }
      int rowSize = ColumnarTable.this.headers.size();
      List<Object> r = new ArrayList<>(rowSize);
      for (int i = 0; i < rowSize; i++) {
        r.add(ColumnarTable.this.values.get(i).get(this.current));
      }
      this.current++;
      return r;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ColumnarTable lists = (ColumnarTable) o;
    return this.headers.equals(lists.headers) && this.measures.equals(lists.measures) && this.values.equals(lists.values);
  }

  @Override
  public int hashCode() {
    return Objects.hash(this.headers, this.measures, this.values);
  }
}
