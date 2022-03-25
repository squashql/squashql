Request:

GET http://localhost:8080/spark-metadata?dataset=itm&repo-url=https%3A%2F%2Fraw.githubusercontent.com%2Fpaulbares%2Faitm-assets%2Fmain%2Fmetrics.json

Response:
```json
{
    "aggregation_functions": [
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
    "metrics": [
        {
            "alias": "indice-prix",
            "expression": "sum(capdv) / sum(competitor_price * quantity)"
        },
        {
            "alias": "marge",
            "expression": "sum(`marge`)"
        }
    ],
    "stores": [
        {
            "name": "their_prices",
            "fields": [
                {
                    "name": "competitor_ean",
                    "type": "string"
                },
                {
                    "name": "competitor_concurrent_pdv",
                    "type": "string"
                },
                {
                    "name": "competitor_brand",
                    "type": "string"
                },
                {
                    "name": "competitor_concurrent_ean",
                    "type": "string"
                },
                {
                    "name": "competitor_price",
                    "type": "double"
                },
                {
                    "name": "scenario",
                    "type": "string"
                }
            ]
        },
        {
            "name": "our_prices",
            "fields": [
                {
                    "name": "ean",
                    "type": "string"
                },
                {
                    "name": "pdv",
                    "type": "string"
                },
                {
                    "name": "price",
                    "type": "double"
                },
                {
                    "name": "quantity",
                    "type": "int"
                },
                {
                    "name": "capdv",
                    "type": "double"
                },
                {
                    "name": "scenario",
                    "type": "string"
                }
            ]
        },
        {
            "name": "our_stores_their_stores",
            "fields": [
                {
                    "name": "our_store",
                    "type": "string"
                },
                {
                    "name": "their_store",
                    "type": "string"
                },
                {
                    "name": "scenario",
                    "type": "string"
                }
            ]
        }
    ]
}
```

Request:
POST http://localhost:8080/spark-query-scenario-grouping?dataset=itm

Payload: 
```json
{
    "table": {
        "name": "our_prices",
        "joins": [
            {
                "table": {
                    "name": "our_stores_their_stores",
                    "joins": [
                        {
                            "table": {
                                "name": "their_prices"
                            },
                            "type": "inner",
                            "mappings": [
                                {
                                    "from": "their_store",
                                    "to": "competitor_concurrent_pdv"
                                }
                            ]
                        }
                    ]
                },
                "type": "inner",
                "mappings": [
                    {
                        "from": "pdv",
                        "to": "our_store"
                    }
                ]
            }
        ]
    },
    "groups": {
        "group1": [
            "base",
            "MN up"
        ],
        "group2": [
            "base",
            "MDD up"
        ],
        "group3": [
            "base",
            "MN & MDD up",
            "MN & MDD down"
        ]
    },
    "comparisons": [
        {
            "method": "absolute_difference",
            "measure": {
                "alias": "indice-prix"
            },
            "show_value": true,
            "reference_position": "previous"
        }
    ],
    "context": {
        "repository": {
            "url": "https://raw.githubusercontent.com/paulbares/aitm-assets/main/metrics.json"
        }
    }
}
```

Response:
```json
{"columns":["group","scenario","absolute_difference(indice-prix, previous)","indice-prix"],"rows":[["group1","base",0.0,0.9803921568627451],["group1","MN up",0.04901960784313719,1.0294117647058822],["group2","base",0.0,0.9803921568627451],["group2","MDD up",0.04901960784313719,1.0294117647058822],["group3","base",0.0,0.9803921568627451],["group3","MN & MDD up",0.0980392156862745,1.0784313725490196],["group3","MN & MDD down",-0.196078431372549,0.8823529411764706]]}
```
