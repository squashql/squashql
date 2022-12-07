package me.paulbares.template;

import me.paulbares.TestClass;

import static me.paulbares.template.ClassTemplateGenerator.generateTestClasses;

public class BigQueryClassTemplateGenerator {

  public static void main(String[] args) throws Exception {
    generateTestClasses(TestClass.Type.BIGQUERY);
  }
}
