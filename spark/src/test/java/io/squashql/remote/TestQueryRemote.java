package io.squashql.remote;

import com.github.dockerjava.api.command.LogContainerCmd;
import io.squashql.SparkDatastore;
import io.squashql.query.AggregatedMeasure;
import io.squashql.query.QueryExecutor;
import io.squashql.query.database.SparkQueryEngine;
import io.squashql.query.dto.QueryDto;
import io.squashql.store.Datastore;
import io.squashql.table.Table;
import io.squashql.transaction.SparkDataLoader;
import io.squashql.type.TableTypedField;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.ContainerLaunchException;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.FrameConsumerResultCallback;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.output.WaitingConsumer;
import org.testcontainers.containers.wait.strategy.AbstractWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Predicate;

import static io.squashql.query.TableField.tableField;
import static io.squashql.transaction.DataLoader.MAIN_SCENARIO_NAME;
import static io.squashql.transaction.DataLoader.SCENARIO_FIELD_NAME;
import static org.testcontainers.containers.output.OutputFrame.OutputType.STDERR;
import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;

@Testcontainers
@Disabled(value = "issue with this test. Investigate later why it suddenly never ends")
public class TestQueryRemote {

//  static {
//    Logger root = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
//    root.setLevel(Level.INFO);
//  }

  public Network network = Network.newNetwork();

  @Container
  public GenericContainer sparkMaster = new GenericContainer(DockerImageName.parse("bitnami/spark:latest"))
          .withExposedPorts(7077, 9000)
          .withNetwork(this.network)
          .withEnv(Map.of(
                  "SPARK_MODE", "master",
                  "SPARK_RPC_AUTHENTICATION_ENABLED", "no",
                  "SPARK_RPC_ENCRYPTION_ENABLED", "no",
                  "SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED", "no",
                  "SPARK_SSL_ENABLED", "no"
          ))
          .withNetworkAliases("spark")
          .waitingFor(new LogMessageWaitStrategy().withRegEx("Utils: Successfully started service 'sparkMaster' on " +
                  "port 7077"));

  @Container
  public GenericContainer sparkWorker = new GenericContainer(DockerImageName.parse("bitnami/spark:latest"))
          .withEnv(Map.of(
                  "SPARK_MODE", "worker",
                  "SPARK_MASTER_URL", "spark://spark:7077",
                  "SPARK_WORKER_MEMORY", "1G",
                  "SPARK_WORKER_CORES", "1",
                  "SPARK_RPC_AUTHENTICATION_ENABLED", "no",
                  "SPARK_RPC_ENCRYPTION_ENABLED", "no",
                  "SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED", "no",
                  "SPARK_SSL_ENABLED", "no"
          ))
          .withNetwork(this.network)
          .waitingFor(new LogMessageWaitStrategy().withRegEx("Worker: Successfully registered with master"));


  @Test
  void testQuery() {
    String storeName = "storeName";
    SparkDatastore datastore = (SparkDatastore) createDatastore();
    QueryExecutor executor = new QueryExecutor(new SparkQueryEngine(datastore));
    SparkDataLoader tm = new SparkDataLoader(datastore.spark);

    TableTypedField ean = new TableTypedField(storeName, "ean", String.class);
    TableTypedField category = new TableTypedField(storeName, "category", String.class);
    TableTypedField price = new TableTypedField(storeName, "price", double.class);
    TableTypedField qty = new TableTypedField(storeName, "quantity", int.class);
    tm.createTemporaryTable(storeName, List.of(ean, category, price, qty));

    tm.load(MAIN_SCENARIO_NAME, storeName, List.of(
            new Object[]{"bottle", "drink", 2d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    tm.load("s1", storeName, List.of(
            new Object[]{"bottle", "drink", 4d, 10},
            new Object[]{"cookie", "food", 3d, 20},
            new Object[]{"shirt", "cloth", 10d, 3}
    ));

    QueryDto query = new QueryDto()
            .table(storeName)
            .withColumn(tableField(storeName, SCENARIO_FIELD_NAME))
            .withMeasure(new AggregatedMeasure("p", "price", "sum"))
            .withMeasure(new AggregatedMeasure("q", "quantity", "sum"));
    Table result = executor.executeQuery(query);
    Assertions.assertThat(result).containsExactlyInAnyOrder(
            List.of("base", 15.0d, 33l),
            List.of("s1", 17.0d, 33l));
  }

  protected Datastore createDatastore() {
    String url = String.format("spark://%s:%d", this.sparkMaster.getHost(), this.sparkMaster.getFirstMappedPort());
    SparkConf conf = new SparkConf()
            .setMaster(url)
            .setAppName("Java Spark SQL Example");
    SparkSession spark = SparkSession
            .builder()
            .config(conf)
            .getOrCreate();

    return new SparkDatastore(spark);
  }

  public static class LogMessageWaitStrategy extends AbstractWaitStrategy {

    private String regEx;

    @Override
    protected void waitUntilReady() {
      WaitingConsumer waitingConsumer = new WaitingConsumer();

      LogContainerCmd cmd =
              DockerClientFactory.instance().client().logContainerCmd(this.waitStrategyTarget.getContainerId())
                      .withFollowStream(true)
                      .withSince(0)
                      .withStdOut(true)
                      .withStdErr(true);

      try (FrameConsumerResultCallback callback = new FrameConsumerResultCallback()) {
        callback.addConsumer(STDOUT, waitingConsumer);
        callback.addConsumer(STDERR, waitingConsumer);

        cmd.exec(callback);

        Predicate<OutputFrame> waitPredicate = outputFrame -> outputFrame.getUtf8String().contains(this.regEx);
        try {
          waitingConsumer.waitUntil(waitPredicate, this.startupTimeout.getSeconds(), TimeUnit.SECONDS, 1);
        } catch (TimeoutException e) {
          throw new ContainerLaunchException("Timed out waiting for log output matching '" + this.regEx + "'");
        }
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    public LogMessageWaitStrategy withRegEx(String regEx) {
      this.regEx = regEx;
      return this;
    }
  }
}
