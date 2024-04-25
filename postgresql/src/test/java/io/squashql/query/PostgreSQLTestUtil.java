package io.squashql.query;

import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Properties;

public class PostgreSQLTestUtil {

  protected static final boolean printAll = true;

  protected static final Properties TEST_PROPERTIES = new Properties();
  static {
    TEST_PROPERTIES.setProperty("user", "test");
    TEST_PROPERTIES.setProperty("password", "test");
    TEST_PROPERTIES.setProperty("database", "test");
  }

  protected static PostgreSQLContainer<?> createContainer() {
    return new PostgreSQLContainer<>(DockerImageName.parse("postgres:16.2"))
            .withReuse(true)
            .withLogConsumer(of -> {
              if (printAll) {
                System.out.print("PostgreSQLContainer Container >>>" + of.getUtf8String());
              }
            });
  }
}
