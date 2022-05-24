package me.paulbares.spring;

import me.paulbares.DataLoader;
import me.paulbares.SparkDatastore;
import me.paulbares.jackson.JacksonUtil;
import me.paulbares.query.SparkQueryEngine;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;

@SpringBootApplication
public class ScenarioAnalysisApplication {

  public static void main(String[] args) {
    SpringApplication.run(ScenarioAnalysisApplication.class, args);
  }

  @Bean
  public SparkQueryEngine itmQueryEngine() {
    SparkDatastore ds = DataLoader.createTestDatastoreWithData();
    return new SparkQueryEngine(ds);
  }

  @Bean
  public MappingJackson2HttpMessageConverter jackson2HttpMessageConverter() {
    return new MappingJackson2HttpMessageConverter(JacksonUtil.mapper);
  }

  @Bean
  public WebMvcConfigurer corsConfigurer() {
    return new WebMvcConfigurer() {
      @Override
      public void addCorsMappings(CorsRegistry registry) {
        registry.addMapping("/**").allowedOrigins("*").allowedMethods("*");
      }
    };
  }
}
