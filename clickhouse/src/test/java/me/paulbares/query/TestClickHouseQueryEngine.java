package me.paulbares.query;

import com.clickhouse.client.ClickHouseProtocol;
import me.paulbares.ClickHouseDatastore;
import me.paulbares.store.Datastore;
import me.paulbares.store.Field;
import me.paulbares.transaction.ClickHouseTransactionManager;
import me.paulbares.transaction.TransactionManager;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.function.Function;

import static java.time.temporal.ChronoUnit.SECONDS;

public class TestClickHouseQueryEngine extends ATestQueryEngine {

  protected static final boolean printAll = true;

  protected static final Function<GenericContainer, String> jdbcUrl = c -> String.format("jdbc:clickhouse://%s:%d",
          c.getHost(),
          c.getMappedPort(ClickHouseProtocol.HTTP.getDefaultPort()));

  @Container
  public GenericContainer container = createClickHouseContainer();

  protected static GenericContainer createClickHouseContainer() {
    return new GenericContainer(DockerImageName.parse("yandex/clickhouse-server:latest"))
            .withExposedPorts(ClickHouseProtocol.HTTP.getDefaultPort(), ClickHouseProtocol.GRPC.getDefaultPort())
            .waitingFor(Wait.forHttp("/ping")
                    .forPort(ClickHouseProtocol.HTTP.getDefaultPort())
                    .forStatusCode(200)
                    .withStartupTimeout(Duration.of(60, SECONDS)))
            .withLogConsumer(of -> {
              String s = ((OutputFrame) of).getUtf8String();
              if (printAll) {
                System.out.print("ClickHouseContainer Container >>>" + s);
              }
            });
  }

  @BeforeAll
  @Override
  void setup() {
    this.container.start();
    super.setup();
  }

  @AfterAll
  void tearDown() {
    this.container.stop();
  }

  @Override
  protected void beforeLoading(List<Field> fields) {
    ClickHouseTransactionManager tm = (ClickHouseTransactionManager) this.tm;
    tm.dropAndCreateInMemoryTable(this.storeName, fields);
  }

  @Override
  protected QueryEngine createQueryEngine(Datastore datastore) {
    return new ClickHouseQueryEngine((ClickHouseDatastore) datastore);
  }

  @Override
  protected Datastore createDatastore(String storeName, List<Field> fields) {
    return new ClickHouseDatastore(jdbcUrl.apply(this.container), null);
  }

  @Override
  protected TransactionManager createTransactionManager() {
    return new ClickHouseTransactionManager(((ClickHouseDatastore) this.datastore).dataSource);
  }

  public static String show(ResultSet set) {
    StringBuilder sb = new StringBuilder();
    try {
      int columnCount = set.getMetaData().getColumnCount();

      while (set.next()) {
        for (int i = 0; i < columnCount; i++) {
          sb.append(set.getObject(i + 1)).append(",");
        }
        sb.append(System.lineSeparator());
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return sb.toString();
  }
}
