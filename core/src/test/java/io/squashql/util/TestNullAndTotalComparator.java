package io.squashql.util;

import io.squashql.query.database.QueryEngine;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Comparator;

public class TestNullAndTotalComparator {

  @Test
  void test() {
    Comparator<String> comp = NullAndTotalComparator.nullsLastAndTotalsFirst(Comparator.naturalOrder());

    Assertions.assertEquals(-1, comp.compare("a", "b"));
    Assertions.assertEquals(1, comp.compare("b", "a"));
    Assertions.assertEquals(1, comp.compare(null, "a"));
    Assertions.assertEquals(-1, comp.compare("a", null));
    Assertions.assertEquals(-1, comp.compare(QueryEngine.GRAND_TOTAL, "a"));
    Assertions.assertEquals(1, comp.compare("a", QueryEngine.GRAND_TOTAL));
    Assertions.assertEquals(-1, comp.compare(QueryEngine.TOTAL, "a"));
    Assertions.assertEquals(1, comp.compare("a", QueryEngine.TOTAL));
    Assertions.assertEquals(-1, comp.compare(QueryEngine.TOTAL, null));
    Assertions.assertEquals(1, comp.compare(null, QueryEngine.TOTAL));
  }
}
