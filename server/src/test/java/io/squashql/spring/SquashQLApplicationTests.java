package io.squashql.spring;

import io.squashql.spring.dataset.DatasetTestConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(DatasetTestConfig.class)
class SquashQLApplicationTests {

  @Test
  void contextLoads() {
  }
}
