package me.paulbares.template;

import static me.paulbares.template.ClassTemplateGenerator.generateTestClasses;

public class SparkClassTemplateGenerator {

  public static final String name = "Spark";

  public static void main(String[] args) throws Exception {
    generateTestClasses(name);
  }
}
