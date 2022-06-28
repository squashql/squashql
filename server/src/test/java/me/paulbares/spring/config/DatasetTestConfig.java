package me.paulbares.spring.config;

import me.paulbares.DataLoader;
import me.paulbares.query.SparkQueryEngine;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class DatasetTestConfig {

  @Bean
  @Primary
  public SparkQueryEngine queryEngineForTest() {
    return new SparkQueryEngine(DataLoader.createTestDatastoreWithData());
  }
}
