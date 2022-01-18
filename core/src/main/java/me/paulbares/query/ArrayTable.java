package me.paulbares.query;

import me.paulbares.store.Field;

import java.util.Iterator;
import java.util.List;

public class ArrayTable implements Table {

  protected final List<Field> headers;
  protected final List<List<Object>> rows;

  public ArrayTable(List<Field> headers, List<List<Object>> rows) {
    this.headers = headers;
    this.rows = rows;
  }

  @Override
  public List<Field> fields() {
    return this.headers;
  }

  @Override
  public long count() {
    return this.rows.size();
  }

  @Override
  public Iterator<List<Object>> iterator() {
    return this.rows.iterator();
  }
}
