package io.squashql.util;

import io.squashql.query.dto.NullsOrderDto;
import java.io.Serializable;
import java.util.Comparator;

import static io.squashql.query.database.QueryEngine.GRAND_TOTAL;
import static io.squashql.query.database.QueryEngine.TOTAL;
import static io.squashql.query.database.SqlTranslator.TOTAL_CELL;

public class NullAndTotalComparator<T> implements Comparator<T>, Serializable {

  private final boolean nullsFirst;
  private final boolean totalFirst;
  // if null, non-null Ts are considered equal
  @SuppressWarnings("serial") // Not statically typed as Serializable
  private final Comparator<T> real;

  NullAndTotalComparator(NullsOrderDto nullsOrder, boolean totalFirst, Comparator<? super T> real) {
    this.nullsFirst = nullsOrder == NullsOrderDto.FIRST;
    this.totalFirst = totalFirst;
    this.real = (Comparator<T>) real;
  }

  @Override
  public int compare(T a, T b) {
    if (a == null) {
      return (b == null) ? 0 : (this.nullsFirst ? -1 : 1);
    } else if (b == null) {
      return this.nullsFirst ? 1 : -1;
    } else {
      if (isTotal(a)) {
        if (isTotal(b)) {
          return compareTotals(a, b);
        }
        return this.totalFirst ? -1 : 1;
      } else if (isTotal(b)) {
        if (isTotal(a)) {
          return compareTotals(a, b);
        }
        return this.totalFirst ? 1 : -1;
      }
      return (this.real == null) ? 0 : this.real.compare(a, b);
    }
  }

  private static <T> int compareTotals(T a, T b) {
    if (GRAND_TOTAL.equals(a)) {
      return GRAND_TOTAL.equals(b) ? 0 : -1;
    } else if (TOTAL.equals(a)) {
      return TOTAL.equals(b) ? 0 : 1;
    } else if (TOTAL_CELL.equals(a)) {
      if (TOTAL_CELL.equals(b)) {
        return 0;
      }
      // we should never end up in case where a = TOTAL_CELL and b != TOTAL_CELL
    }
    throw new RuntimeException("Unexpected value a: " + a + ". b: " + b);
  }

  public static <T> boolean isTotal(T a) {
    return TOTAL.equals(a) || GRAND_TOTAL.equals(a) || TOTAL_CELL.equals(a);
  }

  public static <T> NullAndTotalComparator<T> nullsLastAndTotalsFirst(Comparator<? super T> comparator) {
    return new NullAndTotalComparator<>(NullsOrderDto.LAST, true, comparator);
  }

  public static <T> NullAndTotalComparator<T> totalsFirst(Comparator<? super T> comparator, NullsOrderDto nullsOrderDto) {
    return new NullAndTotalComparator<>(nullsOrderDto, true, comparator);
  }

}
