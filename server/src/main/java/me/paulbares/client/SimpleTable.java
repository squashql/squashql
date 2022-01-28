package me.paulbares.client;

import me.paulbares.query.TableUtils;

import java.util.List;

public class SimpleTable {

  public List<String> columns;
  public List<List<Object>> rows;

  public void show() {
    toString();
  }

  @Override
  public String toString() {
    return TableUtils.toString(this.columns, this.rows, Object::toString, Object::toString);
  }
}
