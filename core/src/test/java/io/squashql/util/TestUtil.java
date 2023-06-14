package io.squashql.util;

import org.assertj.core.api.Assertions;
import org.assertj.core.api.ThrowableAssert;
import org.assertj.core.util.Throwables;

public class TestUtil {

  public static ThrowableAssert<Throwable> assertThatThrownBy(ThrowableAssert.ThrowingCallable shouldRaiseThrowable) {
    try {
      shouldRaiseThrowable.call();
      Assertions.fail("should have thrown an exception");
      return null;
    } catch (Throwable t) {
      Throwable rootCause = t.getCause() == null ? t : Throwables.getRootCause(t);
      return new ThrowableAssert<>(rootCause);
    }
  }
}
