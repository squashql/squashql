package me.paulbares.client.http;

import me.paulbares.DataLoader;
import me.paulbares.query.database.SparkQueryEngine;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

@TestConfiguration
public class DatasetTestConfig {

  @Bean
  @Primary
  public SparkQueryEngine queryEngine() {
    return new SparkQueryEngine(DataLoader.createTestDatastoreWithData());
  }
}
