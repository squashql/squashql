package io.squashql.list;

import java.time.LocalDate;
import java.util.ArrayList;

public final class Lists {

  private Lists() {
  }

  public static class DoubleList extends ArrayList<Double> {

    public DoubleList() {
    }

    public DoubleList(int initialCapacity) {
      super(initialCapacity);
    }
  }

  public static class LongList extends ArrayList<Long> {

    public LongList() {
    }

    public LongList(int initialCapacity) {
      super(initialCapacity);
    }
  }

  public static class StringList extends ArrayList<String> {

    public StringList() {
    }

    public StringList(int initialCapacity) {
      super(initialCapacity);
    }
  }

  public static class LocalDateList extends ArrayList<LocalDate> {

    public LocalDateList() {
    }

    public LocalDateList(int initialCapacity) {
      super(initialCapacity);
    }
  }
}
