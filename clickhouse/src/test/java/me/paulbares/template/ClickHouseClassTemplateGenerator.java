package me.paulbares.template;

import static me.paulbares.template.ClassTemplateGenerator.generateTestClasses;

public class ClickHouseClassTemplateGenerator {

  public static final String name = "ClickHouse";

  public static void main(String[] args) throws Exception {
    generateTestClasses(name);
  }
}
