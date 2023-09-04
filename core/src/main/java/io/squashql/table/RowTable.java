package io.squashql.table;

import io.squashql.query.Header;
import io.squashql.query.Measure;
import io.squashql.query.dictionary.ObjectArrayDictionary;

import java.util.*;

import static io.squashql.query.database.AQueryEngine.TOTAL_COUNT_DEFAULT_VALUE;

public class RowTable implements Table {

  protected final List<Header> headers;
  protected final List<List<Object>> rows;
  private final long totalCount;

  public RowTable(List<Header> headers, List<List<Object>> rows, long totalCount) {
    this.headers = new ArrayList<>(headers);
    this.rows = new ArrayList<>(rows);
    this.totalCount = totalCount;
  }

  /**
   * For tests.
   */
  public RowTable(List<Header> headers, List<List<Object>> rows) {
    this(headers, rows, TOTAL_COUNT_DEFAULT_VALUE);
  }

  private static void throwNotSupportedException() {
    throw new RuntimeException("not supported");
  }

  @Override
  public void addAggregates(Header header, Measure measure, List<Object> values) {
    throwNotSupportedException();
  }

  @Override
  public List<Object> getColumn(int columnIndex) {
    throwNotSupportedException();
    return null;
  }

  @Override
  public long count() {
    return this.rows.size();
  }

  @Override
  public long totalCount() {
    return this.totalCount;
  }

  @Override
  public ObjectArrayDictionary pointDictionary() {
    throwNotSupportedException();
    return null;
  }

  @Override
  public Set<Measure> measures() {
    return Collections.emptySet();
  }

  @Override
  public List<Header> headers() {
    return this.headers;
  }

  @Override
  public void show(int numRows) {
    System.out.println(this);
  }

  @Override
  public String toString() {
    return TableUtils.toString(this.headers, this, h -> ((Header) h).name(), String::valueOf);
  }

  @Override
  public Iterator<List<Object>> iterator() {
    return this.rows.iterator();
  }
}
