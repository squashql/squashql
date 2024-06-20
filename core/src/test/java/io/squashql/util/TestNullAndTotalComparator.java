package io.squashql.util;

import io.squashql.query.dto.NullsOrderDto;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Comparator;

import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;

public class TestNullAndTotalComparator {

  @Test
  void testNullsLastAndTotalsFirst() {
    Comparator<String> comp = NullAndTotalComparator.nullsLastAndTotalsFirst(Comparator.naturalOrder());

    Assertions.assertEquals(-1, comp.compare("a", "b"));
    Assertions.assertEquals(1, comp.compare("b", "a"));
    Assertions.assertEquals(1, comp.compare(null, "a"));
    Assertions.assertEquals(-1, comp.compare("a", null));
    Assertions.assertEquals(-1, comp.compare(GRAND_TOTAL, "a"));
    Assertions.assertEquals(1, comp.compare("a", GRAND_TOTAL));
    Assertions.assertEquals(-1, comp.compare(TOTAL, "a"));
    Assertions.assertEquals(1, comp.compare("a", TOTAL));
    Assertions.assertEquals(-1, comp.compare(TOTAL, null));
    Assertions.assertEquals(1, comp.compare(null, TOTAL));
    Assertions.assertEquals(0, comp.compare(GRAND_TOTAL, GRAND_TOTAL));
    Assertions.assertEquals(0, comp.compare(TOTAL, TOTAL));
    Assertions.assertEquals(-1, comp.compare(GRAND_TOTAL, TOTAL));
    Assertions.assertEquals(1, comp.compare(TOTAL, GRAND_TOTAL));
  }

  @Test
  void testTotalsFirstNullsFirstCase() {
    Comparator<String> comp = NullAndTotalComparator.totalsFirst(Comparator.naturalOrder(), NullsOrderDto.FIRST);

    Assertions.assertEquals(-1, comp.compare("a", "b"));
    Assertions.assertEquals(1, comp.compare("b", "a"));
    Assertions.assertEquals(-1, comp.compare(null, "a"));
    Assertions.assertEquals(1, comp.compare("a", null));
    Assertions.assertEquals(-1, comp.compare(GRAND_TOTAL, "a"));
    Assertions.assertEquals(1, comp.compare("a", GRAND_TOTAL));
    Assertions.assertEquals(-1, comp.compare(TOTAL, "a"));
    Assertions.assertEquals(1, comp.compare("a", TOTAL));
    Assertions.assertEquals(1, comp.compare(TOTAL, null));
    Assertions.assertEquals(-1, comp.compare(null, TOTAL));
    Assertions.assertEquals(0, comp.compare(GRAND_TOTAL, GRAND_TOTAL));
    Assertions.assertEquals(0, comp.compare(TOTAL, TOTAL));
    Assertions.assertEquals(-1, comp.compare(GRAND_TOTAL, TOTAL));
    Assertions.assertEquals(1, comp.compare(TOTAL, GRAND_TOTAL));
  }

  @Test
  void testTotalsFirstNullsLastCase() {
    Comparator<String> comp = NullAndTotalComparator.totalsFirst(Comparator.naturalOrder(), NullsOrderDto.LAST);

    Assertions.assertEquals(-1, comp.compare("a", "b"));
    Assertions.assertEquals(1, comp.compare("b", "a"));
    Assertions.assertEquals(1, comp.compare(null, "a"));
    Assertions.assertEquals(-1, comp.compare("a", null));
    Assertions.assertEquals(-1, comp.compare(GRAND_TOTAL, "a"));
    Assertions.assertEquals(1, comp.compare("a", GRAND_TOTAL));
    Assertions.assertEquals(-1, comp.compare(TOTAL, "a"));
    Assertions.assertEquals(1, comp.compare("a", TOTAL));
    Assertions.assertEquals(-1, comp.compare(TOTAL, null));
    Assertions.assertEquals(1, comp.compare(null, TOTAL));
    Assertions.assertEquals(0, comp.compare(GRAND_TOTAL, GRAND_TOTAL));
    Assertions.assertEquals(0, comp.compare(TOTAL, TOTAL));
    Assertions.assertEquals(-1, comp.compare(GRAND_TOTAL, TOTAL));
    Assertions.assertEquals(1, comp.compare(TOTAL, GRAND_TOTAL));
  }
}
