package io.squashql.query;

import io.squashql.PostgreDatastore;
import io.squashql.query.database.PostgreQueryEngine;
import io.squashql.query.database.QueryEngine;
import io.squashql.store.Datastore;
import io.squashql.template.PostgreClassTemplateGenerator;
import io.squashql.transaction.DataLoader;
import io.squashql.transaction.PostgreDataLoader;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.PostgreSQLContainer;

import static io.squashql.query.PostgreTestUtil.TEST_PROPERTIES;
import static io.squashql.query.PostgreTestUtil.createContainer;

/**
 * Do not edit this class, it has been generated automatically by {@link PostgreClassTemplateGenerator}.
 */
public class TestPostgreQueryExecutor extends ATestQueryExecutor {

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
    ((PostgreDataLoader) this.tm).dropTables(this.fieldsByStore.keySet());
  }

  @Override
  protected void createTables() {
    PostgreDataLoader dl = (PostgreDataLoader) this.tm;
    this.fieldsByStore.forEach(dl::dropAndCreateInMemoryTable);
  }

  @Override
  protected QueryEngine<?> createQueryEngine(Datastore datastore) {
    return new PostgreQueryEngine((PostgreDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore() {
    return new PostgreDatastore(this.container.getJdbcUrl(), TEST_PROPERTIES);
  }

  @Override
  protected DataLoader createDataLoader() {
    return new PostgreDataLoader((PostgreDatastore) this.datastore);
  }
}
