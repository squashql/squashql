package me.paulbares.spring.dataset;

import me.paulbares.BigQueryDatastore;
import me.paulbares.BigQueryUtil;
import me.paulbares.query.database.BigQueryEngine;
import me.paulbares.query.database.QueryEngine;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class DatasetConfig {

  String credendialsPath = "/Users/paul/dev/aitmindiceprix-686299293f2f.json";
  String projectId = "aitmindiceprix";
  String datasetName = "optiprix";

  @Bean
  @ConditionalOnMissingBean
  public QueryEngine queryEngine() {
    BigQueryDatastore datastore = new BigQueryDatastore(BigQueryUtil.createCredentials(this.credendialsPath), this.projectId, this.datasetName);
    return new BigQueryEngine(datastore);
//    return new SparkQueryEngine(SaaSUseCaseDataLoader.createTestDatastoreWithData());
  }
}
