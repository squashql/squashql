package me.paulbares.client.http;

import me.paulbares.DataLoader;
import me.paulbares.query.database.SparkQueryEngine;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;

@TestConfiguration
public class DatasetTestConfig {

  @Bean
  public SparkQueryEngine queryEngine() {
    return new SparkQueryEngine(DataLoader.createTestDatastoreWithData());
  }
}
