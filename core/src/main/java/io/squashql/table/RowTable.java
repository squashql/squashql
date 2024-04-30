package io.squashql.table;

import io.squashql.query.Header;
import io.squashql.query.compiled.CompiledMeasure;
import io.squashql.query.dictionary.ObjectArrayDictionary;

import java.util.*;

public class RowTable implements Table {

  protected final List<Header> headers;
  protected final List<List<Object>> rows;

  public RowTable(List<Header> headers, List<List<Object>> rows) {
    this.headers = new ArrayList<>(headers);
    this.rows = new ArrayList<>(rows);
  }

  private static void throwNotSupportedException() {
    throw new RuntimeException("not supported");
  }

  @Override
  public void addAggregates(Header header, CompiledMeasure measure, List<Object> values) {
    throwNotSupportedException();
  }

  @Override
  public void transferAggregates(Table from, CompiledMeasure measure) {
    throwNotSupportedException();
  }

  @Override
  public List<Object> getColumn(int columnIndex) {
    throwNotSupportedException();
    return null;
  }

  @Override
  public int count() {
    return this.rows.size();
  }

  @Override
  public ObjectArrayDictionary pointDictionary() {
    throwNotSupportedException();
    return null;
  }

  @Override
  public Set<CompiledMeasure> measures() {
    return Collections.emptySet();
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
    return this.rows.iterator();
  }
}
