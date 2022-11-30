package me.paulbares.template;

import com.google.common.io.Files;
import com.google.common.reflect.ClassPath;
import me.paulbares.TestClass;

import java.io.File;
import java.net.URL;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ClassTemplateGenerator {

  public static void main(String[] args) throws Exception {
    List<ClassPath.ClassInfo> parentTestClasses = ClassPath.from(ClassLoader.getSystemClassLoader())
            .getTopLevelClasses("me.paulbares.query")
            .stream()
            .filter(c -> {
              try {
                Class<?> klass = Class.forName(c.getName());
                return klass.getAnnotation(TestClass.class) != null;
              } catch (Throwable e) {
                throw new RuntimeException(e);
              }
            })
            .toList();

    URL resource = ClassTemplateGenerator.class.getClassLoader().getResource("templates/TestClickHouseTemplate.template.java");
    List<String> lines = Files.readLines(new File(resource.toURI()), UTF_8);

    String prefix = "TestClickHouse";
    for (ClassPath.ClassInfo parentTestClass : parentTestClasses) {
      String classSuffix = parentTestClass.getSimpleName().replace("ATest", "");
      System.out.println();
    }

    System.out.println();
  }
}
