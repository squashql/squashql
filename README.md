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
/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/java --add-opens=java.base/sun.nio.ch=ALL-UNNAMED -Ddataset.path=saas.csv -jar server/target/aitm-server-0.1-SNAPSHOT.jar
```
Do not forget to change the path to the file in the above command: `-Ddataset.path=/Users/paul/Downloads/saas.csv`

Server address is: `http://localhost:8080`

## BigQuery

You need to generate a key for your project. Go to [https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries](https://cloud.google.com/bigquery/docs/quickstarts/quickstart-client-libraries)

### Optiprix
```
$JAVA_HOME/bin/java -Dspring.profiles.active=optiprix -Dbigquery.credentials.path=/Users/paul/dev/aitmindiceprix-686299293f2f.json -jar server/target/aitm-server-0.1-SNAPSHOT.jar
```

### CDG
```
$JAVA_HOME/bin/java -Dspring.profiles.active=cdg -Dbigquery.credentials.path=/path/to/your/key.json -jar server/target/aitm-server-0.1-SNAPSHOT.jar
```

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

## GCloud

Check which account is used
```
gcloud auth list
```

Check the project
```
gcloud config list project
```

Deploy
```
mvn -Pgcloud-cdg -DskipTests package appengine:deploy
```

Read the logs
```
gcloud app logs tail -s default
```

### Deploy from local machine

```
mvn -pl :aitm-server -am clean install -DskipTests -Pspring-boot \
&& mvn -pl :aitm-server -Pgcloud-cdg -DskipTests package appengine:deploy
```

### Deploy from CloudBuild

Check the file cloudbuild.yaml in the root directory. The service account used by Cloud Build needs the following roles:
- App Engine Admin
- App Engine Deployer
- Cloud Build Service Account
- Service Account User 
