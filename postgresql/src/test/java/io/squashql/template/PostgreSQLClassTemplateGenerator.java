package io.squashql.template;

import io.squashql.TestClass;

import static io.squashql.template.ClassTemplateGenerator.generateTestClasses;

public class PostgreSQLClassTemplateGenerator {

  public static void main(String[] args) throws Exception {
    generateTestClasses(TestClass.Type.POSTGRESQL);
  }
}
