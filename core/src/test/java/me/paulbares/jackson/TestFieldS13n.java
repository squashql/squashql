package me.paulbares.jackson;

import me.paulbares.store.Field;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;

public class TestFieldS13n {

  @Test
  void test() {
    Arrays.asList(String.class,
            Double.class,
            double.class,
            Integer.class,
            int.class,
            Float.class,
            float.class,
            Long.class,
            long.class).stream().forEach(this::checkForType);
  }

  protected void checkForType(Class<?> type) {
    String name = "fieldName";
    Field fieldName = new Field(name, type);
    String format = "{\"name\":\"%s\",\"type\":\"%s\"}";
    Assertions.assertThat(JacksonUtil.serialize(fieldName)).isEqualTo(format.formatted(name, type.getSimpleName().toLowerCase()));
  }
}
