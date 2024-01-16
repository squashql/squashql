package io.squashql.jackson;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;

public class TestJacksonUtil {

  @Test
  void testLocalDate() {
    LocalDate d = LocalDate.of(2000, 3, 15);
    String serialize = JacksonUtil.serialize(d);
    LocalDate deserialize = JacksonUtil.deserialize(serialize, LocalDate.class);
    Assertions.assertThat(deserialize).isEqualTo(d);
  }
}
