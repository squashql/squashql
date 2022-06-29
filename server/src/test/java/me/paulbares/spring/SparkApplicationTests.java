package me.paulbares.spring;

import me.paulbares.spring.config.DatasetTestConfig;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;

@SpringBootTest
@Import(DatasetTestConfig.class)
class SparkApplicationTests {

  @Test
  void contextLoads() {
  }
}
