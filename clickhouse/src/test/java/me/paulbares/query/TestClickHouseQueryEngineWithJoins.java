package me.paulbares.query;

import me.paulbares.ClickHouseDatastore;
import me.paulbares.query.database.ClickHouseQueryEngine;
import me.paulbares.query.database.QueryEngine;
import me.paulbares.store.Datastore;
import me.paulbares.transaction.ClickHouseTransactionManager;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;

import static me.paulbares.query.TestUtils.createClickHouseContainer;
import static me.paulbares.query.TestUtils.jdbcUrl;

@Disabled
public class TestClickHouseQueryEngineWithJoins extends ATestQueryEngineWithJoins {

    @Container
    public GenericContainer container = createClickHouseContainer();

    @BeforeAll
    @Override
    void setup() {
        this.container.start();
        super.setup();
    }

    @Override
    protected QueryEngine createQueryEngine(Datastore datastore) {
        return new ClickHouseQueryEngine((ClickHouseDatastore) datastore);
    }

    @Override
    protected Datastore createDatastore() {
        return new ClickHouseDatastore(jdbcUrl.apply(this.container), null);
    }

    @Override
    protected TransactionManager createTransactionManager() {
        return new ClickHouseTransactionManager(((ClickHouseDatastore) this.datastore).dataSource);
    }
}
