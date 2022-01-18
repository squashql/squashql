package me.paulbares.query;

import java.util.List;

public interface Table extends Iterable<List<Object>> {

  List<String> headers();

  long rowCount();
}
