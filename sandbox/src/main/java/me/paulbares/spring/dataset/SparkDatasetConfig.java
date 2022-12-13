package me.paulbares.spring.dataset;

import me.paulbares.SaasUseCaseDataLoader;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.query.database.SparkQueryEngine;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * JVM parameters: -Dspring.profiles.active=spark --add-opens=java.base/sun.nio.ch=ALL-UNNAMED
 */
@Configuration
@Profile("spark")
public class SparkDatasetConfig {

  @Bean
  @ConditionalOnMissingBean // This is for the tests. Tests can use another one if defined.
  public QueryEngine queryEngine() {
    return new SparkQueryEngine(SaasUseCaseDataLoader.createTestDatastoreWithData());
  }
}
