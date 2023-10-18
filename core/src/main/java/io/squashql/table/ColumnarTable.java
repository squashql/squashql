package io.squashql.table;

import com.google.common.base.Suppliers;
import io.squashql.query.Header;
import io.squashql.query.Measure;
import io.squashql.query.dictionary.ObjectArrayDictionary;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

public class ColumnarTable implements Table {

  protected final List<Header> headers;
  protected final Set<Measure> measures;

  public final Supplier<ObjectArrayDictionary> pointDictionary;
  protected final List<List<Object>> values;

  public ColumnarTable(List<Header> headers, Set<Measure> measures, List<List<Object>> values) {
    if (headers.stream().filter(Header::isMeasure)
            .anyMatch(measureHeader -> !measures.stream().map(Measure::alias).toList()
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
  public void addAggregates(Header header, Measure measure, List<Object> values) {
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
  public long count() {
    return this.values.get(0).size();
  }

  @Override
  public ObjectArrayDictionary pointDictionary() {
    return this.pointDictionary.get();
  }

  @Override
  public Set<Measure> measures() {
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
  public Iterator<List<Object>> iterator() {
    return new ColumnarTableIterator();
  }

  private class ColumnarTableIterator implements Iterator<List<Object>> {

    int current = 0;
    long size = count();

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
