package me.paulbares.util;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestTableTSCodeGenerator {

  @Test
  void testTransformName() {
    String expected = "helloWorldAbc";
    String expectedUpperCase = "HelloWorldAbc";
//    Assertions.assertThat(TableTSCodeGenerator.transformName("hello.world_abc", false)).isEqualTo(expected);
//    Assertions.assertThat(TableTSCodeGenerator.transformName("hello.world_abc", true)).isEqualTo(expectedUpperCase);
//    Assertions.assertThat(TableTSCodeGenerator.transformName("helloWorldAbc", false)).isEqualTo(expected);
//    Assertions.assertThat(TableTSCodeGenerator.transformName("helloWorldAbc", true)).isEqualTo(expectedUpperCase);
    Assertions.assertThat(TableTSCodeGenerator.transformName(".hello.WorldAbc", false)).isEqualTo(expected);
//    Assertions.assertThat(TableTSCodeGenerator.transformName(".hello.WorldAbc", true)).isEqualTo(expectedUpperCase);
//    Assertions.assertThat(TableTSCodeGenerator.transformName("helloWorldAbc.", false)).isEqualTo(expected);
//    Assertions.assertThat(TableTSCodeGenerator.transformName("helloWorldAbc.", true)).isEqualTo(expectedUpperCase);
  }
}
