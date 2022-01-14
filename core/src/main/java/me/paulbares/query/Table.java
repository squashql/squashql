package me.paulbares.query;

import java.util.Iterator;
import java.util.List;

public interface Table {

  List<String> headers();

  Iterator<List<Object>> rowIterator();

  long rowCount();
}
