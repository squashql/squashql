package io.squashql.query;

import io.squashql.PostgreSQLDatastore;
import io.squashql.query.database.PostgreSQLQueryEngine;
import io.squashql.query.database.QueryEngine;
import io.squashql.store.Datastore;
import io.squashql.template.PostgreSQLClassTemplateGenerator;
import io.squashql.transaction.DataLoader;
import io.squashql.transaction.PostgreSQLDataLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

import static io.squashql.query.PostgreSQLTestUtil.TEST_PROPERTIES;
import static io.squashql.query.PostgreSQLTestUtil.createContainer;

/**
 * Do not edit this class, it has been generated automatically by {@link PostgreSQLClassTemplateGenerator}.
 */
public class TestPostgreSQLGrandTotalAlongAncestorsComparison extends ATestGrandTotalAlongAncestorsComparison {

  private final PostgreSQLContainer<?> container = createContainer();

  @BeforeAll
  @Override
  void setup() {
    this.container.start();
    super.setup();
  }

  @AfterAll
  void tearDown() {
    // we do not stop the container to be able to reuse it between tests.
    ((PostgreSQLDataLoader) this.tm).dropTables(this.fieldsByStore.keySet());
  }

  @Override
  protected void createTables() {
    PostgreSQLDataLoader dl = (PostgreSQLDataLoader) this.tm;
    this.fieldsByStore.forEach(dl::dropAndCreateInMemoryTable);
  }

  @Override
  protected QueryEngine<?> createQueryEngine(Datastore datastore) {
    return new PostgreSQLQueryEngine((PostgreSQLDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    return new PostgreSQLDatastore(this.container.getJdbcUrl(), TEST_PROPERTIES);
  }

  @Override
  protected DataLoader createDataLoader() {
      return new PostgreSQLDataLoader((PostgreSQLDatastore) this.datastore);
    }
}
