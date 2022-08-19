## Prerequisites

In order to build the server, you will need:
- [Java JDK](https://www.oracle.com/java/) >= 17
- Latest stable [Apache Maven](http://maven.apache.org/)

## Run locally

- Install prerequisites (see above)
- Build the project
```
mvn -pl :aitm-server -am clean install -DskipTests -Pspring-boot
```
- Launch the project with the following command. Replace `/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/java` 
by your java path if necessary. 
```
/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Ddataset.path=/Users/paul/Downloads/saas.csv -jar server/target/aitm-server-0.1-SNAPSHOT.jar
```
Do not forget to change the path to the file in the above command: `-Ddataset.path=/Users/paul/Downloads/saas.csv`

Server address is: `http://localhost:8080`

## JShell

To interactively interact with the server and execute queries, one can use jshell. To do that, compile the module 
`http-client` with the jshell profile:

```
mvn -pl :http-client -am clean install -DskipTests -Pjshell
$JAVA_HOME/bin/jshell --class-path target/http-client-0.1-SNAPSHOT.jar
```

```jshelllanguage
import me.paulbares.client.http.*
import static me.paulbares.query.QueryBuilder.*

var querier = new HttpClientQuerier("http://localhost:8080")

querier.metadata()

var query = query()
var products = table("products")

query.table(products)

query.wildcardCoordinate("scenario").aggregatedMeasure("marge", "sum")

querier.run(query)

query.wildcardCoordinate("type-marque")

query.context("totals", TOP)

query.condition("type-marque", eq("MDD"))
```

For Grouping queries:

```jshelllanguage
import me.paulbares.client.*
import static me.paulbares.query.QueryBuilder.*

var querier = new HttpClientQuerier("http://localhost:8080")

querier.metadata()

var query = scenarioComparisonQuery()
var products = table("products")

query.table(products)

query.defineNewGroup("group1", "base", "mdd-baisse")
query.defineNewGroup("group2", "base", "mdd-baisse-simu-sensi")
query.defineNewGroup("group3", "base", "mdd-baisse", "mdd-baisse-simu-sensi")

var comp = comparison("absolute_difference", aggregatedMeasure("marge", "sum"), false, "first")

query.addScenarioComparison(comp)

querier.run(query)

query.json()

comp.showValue = true

querier.run(query)
```

## REMOTE SPARK CLUSTER

By default, an embedded spark cluster is used, but you can configure the `Datastore` to use a remote cluster. To do that,
simply pass your own `SparkSession` when creating `SparkDatastore` object. See `TestQueryRemote` as an example.
