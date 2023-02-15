package io.squashql.query;

import io.squashql.query.dictionary.ObjectArrayDictionary;
import io.squashql.store.Field;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

public class RowTable implements Table {

  protected final List<Field> headers;
  protected final List<List<Object>> rows;

  public RowTable(List<Field> headers, List<List<Object>> rows) {
    this.headers = new ArrayList<>(headers);
    this.rows = new ArrayList<>(rows);
  }

  private static void throwNotSupportedException() {
    throw new RuntimeException("not supported");
  }

  @Override
  public void addAggregates(Field field, Measure measure, List<Object> values) {
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
  public ObjectArrayDictionary pointDictionary() {
    throwNotSupportedException();
    return null;
  }

  @Override
  public List<Measure> measures() {
    return Collections.emptyList();
  }

  @Override
  public int[] measureIndices() {
    return new int[0];
  }

  @Override
  public int[] columnIndices() {
    return IntStream.range(0, this.headers.size()).toArray();
  }

  @Override
  public List<Field> headers() {
    return this.headers;
  }

  @Override
  public void show(int numRows) {
    System.out.println(this);
  }

  @Override
  public String toString() {
    return TableUtils.toString(this.headers, this, f -> ((Field) f).name(), String::valueOf);
  }

  @Override
  public Iterator<List<Object>> iterator() {
    return this.rows.iterator();
  }
}
