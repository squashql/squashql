package me.paulbares.template;

import me.paulbares.TestClass;

import static me.paulbares.template.ClassTemplateGenerator.generateTestClasses;

public class ClickHouseClassTemplateGenerator {

  public static void main(String[] args) throws Exception {
    generateTestClasses(TestClass.Type.CLICKHOUSE);
  }
}
