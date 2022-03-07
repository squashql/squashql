## Prerequisites

In order to build the server, you will need:
- [Java JDK](https://www.oracle.com/java/) >= 17
- Latest stable [Apache Maven](http://maven.apache.org/)

## Heroku CLI

```
heroku config:set JAVA_OPTS="-Xmx512m --add-opens=java.base/sun.nio.ch=ALL-UNNAMED" -a sa-mvp
heroku config:set PORT="122021" -a sa-mvp

heroku logs --tail -a sa-mvp
```

## REST API

### DEV URL
- To check if the server is up and running, open: https://sa-mvp.herokuapp.com/ It can take 1 minute or so because it is hosted on Heroku and uses a free account that is turned off after a period of inactivity. Once up, a message will appear. 
- To execute a query, send a POST request to https://sa-mvp.herokuapp.com/spark-query. See payload example below. 
- To execute a "scenario grouping" query, send a POST request to https://sa-mvp.herokuapp.com/spark-query-scenario-grouping. See payload example below. 
- To get the metadata of the store (to know the fields that can be queried and the list of supported aggregation functions): send a GET request to https://sa-mvp.herokuapp.com/spark-metadata. See response example below

To work with the new dataset (the one with 4 scenarios and two tables), append `?dataset=itm` to urls. For instance:
`https://sa-mvp.herokuapp.com/spark-metadata?dataset=itm`. For _scenario grouping queries_ (url `https://sa-mvp.herokuapp.com/spark-query-scenario-grouping?dataset=itm`), here's a query example:

```json
{"groups":{"group1":["base","MDD up","MN up"],"group2":["base","MN & MDD down"],"group3":["base","MN up","MDD up","MN & MDD down"]},"comparisons":[{"method":"absolute_difference","measure":{"alias":"indice_prix","expression":"sum(capdv) / sum(competitor_price * quantity)"},"show_value":true,"reference_position":"first"}],"table":{"name":"our_prices","joins":[{"table":{"name":"their_prices"},"type":"inner","mappings":[{"from":"ean","to":"competitor_ean"}]}]}}
```

Try some requests with Postman: https://www.postman.com/pba9472/workspace/aitm/collection/10808486-6f1b6549-272c-4780-9149-fe8316ac268d

### Specification

**Coordinates** are key/value pairs. 
Key refers to a field in the table. Possible values: ean, pdv, categorie, type-marque, sensibilite, quantite, prix, achat, score-visi, min-marche Value refers to the desired values for which the aggregates must be computed. It can be null (wildcard) to indicate all values must be returned or an array (of length >= 1) of possible values e.g "scenario": ["base", "mdd-baisse"].

**Measures** are either aggregated measures built from a field and an aggregation function (can be sum, min, max, avg) or calculated measures built from an sql expression (because Spark under the hood so it was the easiest way to do). Note the fields in the expression must be quoted with backticks (see example below).

#### Query payload example

Cossjoin of scenario|type-marque, measures are marge.sum and a calculated measure:

Payload:

```json
{
  "table": {
    "name": "products"
  },
  "coordinates": {
    "scenario": null,
    "type-marque": null
  },
  "measures": [
    {
      "field": "marge",
      "aggregation_function": "sum"
    },
    {
      "alias": "indice-prix",
      "expression": "100 * sum(`numerateur-indice`) / sum(`score-visi`)"
    }
  ]
}
```

Response:
```json
{
  "columns":[
    "scenario",
    "type-marque",
    "sum(marge)",
    "indice-prix"
  ],
   "rows":[
      ["base", "MDD", 190.00000000000003, 122.50000000000001],
      ["base", "MN", 90.00000000000003, 104.42477876106196],
      ["mdd-baisse-simu-sensi", "MDD", 100.0, 100.0],
      ["mdd-baisse-simu-sensi", "MN", 90.00000000000003, 104.42477876106196],
      ["mdd-baisse", "MN", 90.00000000000003, 104.42477876106196],
      ["mdd-baisse", "MDD", 150.0, 112.5]
   ]
}
```

Some additional contexts can be provided to enrich or modify the query results. For the moment, only the context value `totals` 
is supported:

```json
...
"context": {
  "totals": {
    "visible": true,
    "position": "top"
  } 
}
...
```
If `totals.visible` is true, the result includes extra rows that represent the subtotals, which are commonly referred to as super-aggregate rows, along with the grand total row.
`totals.position` to change the totals positions in the results. Default is `top`.

```json
{
  "table": {
    "name": "products"
  },
  "coordinates": {
    "scenario": null,
    "type-marque": null
  },
  "measures": [
    {
      "field": "marge",
      "aggregation_function": "sum"
    },
    {
      "alias": "indice-prix",
      "expression": "100 * sum(`numerateur-indice`) / sum(`score-visi`)"
    }
  ],
  "context": {
    "totals": {
      "visible": true,
      "position": "top"
    }
  }
}
```

Response:
```json
{
   "columns":[
      "scenario",
      "type-marque",
      "sum(marge)",
      "indice-prix"
   ],
   "rows":[
      ["Grand Total", null, 710.0000000000001, 106.83874139626353],
      ["base", "Total", 280.00000000000006, 110.44985250737464],
      ["base", "MDD", 190.00000000000003, 122.50000000000001],
      ["base", "MN", 90.00000000000003, 104.42477876106196],
      ["mdd-baisse", "Total", 240.00000000000003, 107.1165191740413],
      ["mdd-baisse", "MDD", 150.0, 112.5],
      ["mdd-baisse", "MN", 90.00000000000003, 104.42477876106196],
      ["mdd-baisse-simu-sensi", "Total", 190.00000000000003, 102.94985250737463],
      ["mdd-baisse-simu-sensi", "MDD", 100.0, 100.0],
      ["mdd-baisse-simu-sensi", "MN", 90.00000000000003, 104.42477876106196]
   ]
}
```

This API can also be used for discovery! For instance to fetch all existing scenario:

Payload:
```json
{
  "table": {
    "name": "products"
  },
  "coordinates": {
    "scenario": null
  }
}
```

Response:
```json
{
   "columns":[
      "scenario"
   ],
   "rows":[
      ["base"],
      ["mdd-baisse-simu-sensi"],
      ["mdd-baisse"]
   ]
}
```

#### Metadata response example

Response:
```json
{
   "aggregation_functions":[
      "sum",
      "min",
      "max",
      "avg",
      "var_samp",
      "var_pop",
      "stddev_samp",
      "stddev_pop",
      "count"
   ],
   "stores":[
      {
         "name":"products",
         "fields":[
            {
               "name":"ean",
               "type":"string"
            },
            {
               "name":"pdv",
               "type":"string"
            },
            {
               "name":"categorie",
               "type":"string"
            },
            {
               "name":"type-marque",
               "type":"string"
            },
            {
               "name":"sensibilite",
               "type":"string"
            },
            {
               "name":"quantite",
               "type":"int"
            },
            {
               "name":"prix",
               "type":"double"
            },
            {
               "name":"achat",
               "type":"int"
            },
            {
               "name":"score-visi",
               "type":"int"
            },
            {
               "name":"min-marche",
               "type":"double"
            },
            {
               "name":"ca",
               "type":"double"
            },
            {
               "name":"marge",
               "type":"double"
            },
            {
               "name":"numerateur-indice",
               "type":"double"
            },
            {
               "name":"indice-prix",
               "type":"double"
            },
            {
               "name":"scenario",
               "type":"string"
            }
         ]
      }
   ]
}
```

#### Scenario Grouping payload example

The use case is explained [in this document.](https://docs.google.com/document/d/1-gPXlpSaoAmkHgZ_lmTmHNqz3CyVehDUzHwRbC9Uw4I/edit?usp=sharing)

`comparisonMethod` can be `ABSOLUTE` or `RELATIVE` to determine which formula to apply to compute cell values

ABSOLUTE: `value = (currentValue - previousValue)` 

RELATIVE: `value = (currentValue - previousValue) / previousValue`

Payload

```json
{
  "table": {
    "name": "products"
  },
  "groups": {
    "group1" : ["base", "mdd-baisse-simu-sensi"],
    "group2" : ["base", "mdd-baisse"],
    "group3" : ["base", "mdd-baisse-simu-sensi", "mdd-baisse"]
  },
   "comparisons":[
      {
         "method":"absolute_difference",
         "measure":{
            "field":"marge",
            "aggregation_function":"sum"
         },
         "show_value":false,
         "reference_position":"previous"
      },
      {
         "method":"absolute_difference",
         "measure":{
           "alias": "indice-prix",
           "expression": "100 * sum(`numerateur-indice`) / sum(`score-visi`)"
         },
         "show_value":false,
         "reference_position":"previous"
      }
   ]
}
```

Response
```json
{
  "columns": ["group","scenario","absolute_difference(sum(marge), previous)","absolute_difference(sum(indice-prix), previous)"],
  "rows": [
    ["group1","base",0.0,0.0],
    ["group1","mdd-baisse-simu-sensi",-90.00000000000003,-7.500000000000014],
    ["group2","base",0.0,0.0],
    ["group2","mdd-baisse",-40.00000000000003,-3.333333333333343],
    ["group3","base",0.0,0.0],
    ["group3","mdd-baisse-simu-sensi",-90.00000000000003,-7.500000000000014],
    ["group3","mdd-baisse",50.0,4.166666666666671]
  ]
}
```

#### Joins

In the payload, you can specify the table on which the query is executed and if multiple necessary other tables joined 
to this "main" table. It is the place where you can express "snowflake schema" style at query time. For instance:

```json
...
  "table": {
    "name": "orders",
    "joins": [
      {
        "table": {
          "name": "orderDetails",
          "joins": [
            {
              "table": {
                "name": "products"
              },
              "type": "inner",
              "mappings": [
                {
                  "from": "productId",
                  "to": "productId"
                }
              ]
            }
          ]
        },
        "type": "inner",
        "mappings": [
          {
            "from": "orderId",
            "to": "orderId"
          }
        ]
      }
    ]
  }
...
```

In this example, `orders` is the base store (fact table). It is linked to `orderDetails` via the orderId field. `orderDetails`
is itself joined to the `products` table via the productId.

Supported type of joins are: `inner` and `left`.
Mapping can be done on multiple fields if necessary.

#### Conditions

Example of a 2 conditions. The first one on the scenarios (single equal condition), the second one on the category field (category equals to drink AND food) 
```json
{
   "table":{
      "name":"products"
   },
   "coordinates":{
      ...
   },
   "conditions":{
      "scenario":{
         "type":"EQ",
         "value":"base"
      },
      "category":{
         "type":"AND",
         "one":{
            "type":"EQ",
            "value":"drink"
         },
         "two":{
            "type":"EQ",
            "value":"food"
         }
      }
   },
   "measures":[
      ...
   ]
}
```

## JShell

To interactively interact with the server and execute queries, one can use jshell. To do that, compile the project with the jshell profile `mvn clean install -Pjshell` and launch jshell by running the executable and adding the required jar to the class-path. For instance:

```
/Library/Java/JavaVirtualMachines/temurin-17.jdk/Contents/Home/bin/jshell --class-path ~/.m2/repository/me/paulbares/aitm-server/0.1-SNAPSHOT/aitm-server-0.1-SNAPSHOT.jar
```

And then (can be saved in a file):
```jshelllanguage
import me.paulbares.client.*
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