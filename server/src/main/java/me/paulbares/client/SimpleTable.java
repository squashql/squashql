package me.paulbares.client;

import me.paulbares.query.TableUtils;

import java.util.List;

public class SimpleTable {

  public List<String> columns;
  public List<List<Object>> rows;

  public void show() {
    System.out.println(this);
  }

  @Override
  public String toString() {
    return TableUtils.toString(this.columns, this.rows, String::valueOf, String::valueOf);
  }
}
