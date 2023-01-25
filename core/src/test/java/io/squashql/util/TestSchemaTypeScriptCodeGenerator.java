package io.squashql.util;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestSchemaTypeScriptCodeGenerator {

  @Test
  void testTransformName() {
    String expected = "helloWorldAbc";
    String expectedUpperCase = "HelloWorldAbc";
    Assertions.assertThat(SchemaTypeScriptCodeGenerator.transformName("hello.world_abc", false)).isEqualTo(expected);
    Assertions.assertThat(SchemaTypeScriptCodeGenerator.transformName("hello.world_abc", true)).isEqualTo(expectedUpperCase);
    Assertions.assertThat(SchemaTypeScriptCodeGenerator.transformName("helloWorldAbc", false)).isEqualTo(expected);
    Assertions.assertThat(SchemaTypeScriptCodeGenerator.transformName("helloWorldAbc", true)).isEqualTo(expectedUpperCase);
    Assertions.assertThat(SchemaTypeScriptCodeGenerator.transformName(".hello.WorldAbc", false)).isEqualTo(expected);
    Assertions.assertThat(SchemaTypeScriptCodeGenerator.transformName(".hello.WorldAbc", true)).isEqualTo(expectedUpperCase);
    Assertions.assertThat(SchemaTypeScriptCodeGenerator.transformName("helloWorldAbc.", false)).isEqualTo(expected);
    Assertions.assertThat(SchemaTypeScriptCodeGenerator.transformName("helloWorldAbc.", true)).isEqualTo(expectedUpperCase);
  }
}
