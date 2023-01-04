package io.squashql;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Annotation to put on abstract test classes for test class generation.
 */
@Retention(RUNTIME)
@Target(TYPE)
public @interface TestClass {

  Type[] ignore() default {};

  enum Type {
    SPARK("Spark"),
    CLICKHOUSE("ClickHouse"),
    BIGQUERY("BigQuery"),
    SNOWFLAKE("Snowflake");

    public final String className;

    Type(String className) {
      this.className = className;
    }
  }
}
