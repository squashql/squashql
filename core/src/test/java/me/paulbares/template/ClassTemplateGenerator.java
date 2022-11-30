package me.paulbares.template;

import com.google.common.io.Files;
import com.google.common.reflect.ClassPath;
import me.paulbares.TestClass;

import java.io.BufferedWriter;
import java.io.File;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static java.nio.charset.StandardCharsets.UTF_8;

public class ClassTemplateGenerator {

  public static void generateTestClasses(String name) throws Exception {
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

    URL resource = ClassTemplateGenerator.class.getClassLoader().getResource("templates/Test" + name + "Template.template.java");
    List<String> lines = Files.readLines(new File(resource.toURI()), UTF_8);

    File rootTestClasses = new File(name.toLowerCase() + "/src/test/java/me/paulbares/query").getAbsoluteFile();

    String prefix = "Test" + name;
    List<Path> classGenerated = new ArrayList<>();
    for (ClassPath.ClassInfo parentTestClass : parentTestClasses) {
      String classSuffix = parentTestClass.getSimpleName().replace("ATest", "");
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
