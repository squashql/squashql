package me.paulbares.spring;

import me.paulbares.DataLoader;
import me.paulbares.SparkDatastore;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.SparkQueryEngine;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;

@SpringBootApplication
public class ScenarioAnalysisApplication {

  public static void main(String[] args) {
    SpringApplication.run(ScenarioAnalysisApplication.class, args);
  }

  @Bean
  public SparkQueryEngine queryEngine() {
    SparkDatastore ds = DataLoader.createTestDatastoreWithData();
    return new SparkQueryEngine(ds);
  }

  @Bean
  public MappingJackson2HttpMessageConverter jackson2HttpMessageConverter() {
    return new MappingJackson2HttpMessageConverter(JacksonUtil.mapper);
  }
}
