package me.paulbares.spring.dataset;

import me.paulbares.SaaSUseCaseDataLoader;
import me.paulbares.query.database.SparkQueryEngine;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatasetConfig {

  @Bean
  @ConditionalOnMissingBean
  public SparkQueryEngine queryEngine() {
    return new SparkQueryEngine(SaaSUseCaseDataLoader.createTestDatastoreWithData());
  }
}
