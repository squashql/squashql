package io.squashql.template;

import com.google.common.io.Files;
import com.google.common.reflect.ClassPath;
import io.squashql.TestClass;

import java.io.BufferedWriter;
import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * To have specific code for each db, create an intermediate class such as ATestDuckDBVectorAggregation extends
 * ATestVectorAggregation. The latter should not be annotated with {@link TestClass} but the former yes. In ATestVectorAggregation,
 * create the abstract method to be used in the tests and override it in ATestDuckDBVectorAggregation.
 */
public class ClassTemplateGenerator {

  public static void generateTestClasses(TestClass.Type testClassType) throws Exception {
    List<ClassPath.ClassInfo> parentTestClasses = ClassPath.from(ClassLoader.getSystemClassLoader())
            .getTopLevelClassesRecursive("io.squashql.query")
            .stream()
            .filter(c -> {
              try {
                Class<?> klass = Class.forName(c.getName());
                TestClass annotation = klass.getAnnotation(TestClass.class);
                if (annotation != null) {
                  TestClass.Type[] ignore = annotation.ignore();
                  if (ignore == null || ignore.length == 0) {
                    return true;
                  } else {
                    return Arrays.stream(ignore).filter(i -> i == testClassType).findAny().isEmpty();
                  }
                } else {
                  return false;
                }
              } catch (Throwable e) {
                throw new RuntimeException(e);
              }
            })
            .toList();

    URL resource = ClassTemplateGenerator.class.getClassLoader().getResource("templates/Test" + testClassType.className + "Template.template.java");
    List<String> lines = Files.readLines(new File(resource.toURI()), UTF_8);

    File rootTestClasses = new File(testClassType.className.toLowerCase() + "/src/test/java/io/squashql/query").getAbsoluteFile();

    String prefix = "Test" + testClassType.className;
    List<Path> classGenerated = new ArrayList<>();
    for (ClassPath.ClassInfo parentTestClass : parentTestClasses) {
      String classSuffix = parentTestClass.getSimpleName().replace("ATest" + testClassType.className, ""); // start with specific parent class
      classSuffix = classSuffix.replace("ATest", "");
      String fileName = prefix + classSuffix + ".java";
      Path path = Paths.get(rootTestClasses.getAbsolutePath(), fileName);
      classGenerated.add(path);
      BufferedWriter writer = Files.newWriter(path.toFile(), UTF_8);
      for (String line : lines) {
        String l = line.replace("{{classSuffix}}", classSuffix);
        l = l.replace("{{parentTestClass}}", parentTestClass.getSimpleName());
        writer.write(l);
        writer.newLine();
      }
      writer.flush();
    }
    System.out.println("Class generated: ");
    classGenerated.forEach(System.out::println);
  }
}
