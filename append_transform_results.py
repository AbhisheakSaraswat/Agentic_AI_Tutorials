from typing import Dict, Any

ETL_SLOT_MAP = {
    "left": {
        "pre": {"aggregate": 0, "filter": 1, "transform": 2},
        "post": {"aggregate": 3, "filter": 4, "transform": 5},
    },
    "right": {
        "pre": {"aggregate": 0, "filter": 1, "transform": 2},
        "post": {"aggregate": 3, "filter": 4, "transform": 5},
    }
}


def insert_into_etl(config: dict, side: str, stage: str, component: str, data: dict):
    """
    Insert data into ETL structure at the correct position.
    
    Args:
        config: the full JSON config (with utilconfig->etl)
        side: 'left' or 'right'
        stage: 'pre' or 'post'
        component: 'aggregate' | 'filter' | 'transform'
        data: dict to insert
    """
    # config = {"utilconfig": {"etl": {}}}
    etl_side = config["utilconfig"]["etl"].setdefault(f"{side}Etl", [])

    while len(etl_side) < 6:
        etl_side.append({})

    slot_index = ETL_SLOT_MAP[side][stage][component]

    if component == "aggregate":
        element = {
            "groupByColumns": data.get("groupByColumns", []),
            "aggregateColumnRules": data.get("aggregateColumnRules", []),
        }
    elif component == "filter":
        element = {
            "filterRules": data.get("filterRules", []),
        }
    elif component == "transform":
        element = {
            "lookUpDataSources": data.get("lookUpDataSources", []),
            "transformRuleSet": data.get("transformRuleSet", {}),
        }
    else:
        raise ValueError(f"Unknown component type: {component}")

    etl_side[slot_index] = element
    return config


# config = {"utilconfig": {"etl": {}}}

# Insert pre-aggregate on left side
# insert_into_etl(config, side="left", stage="pre", component="aggregate", data={
#     "groupByColumns": ["FileId"],
#     "aggregateColumnRules": [{"column": "Count", "aggregatorType": "default", "aggregator": "COUNT"}],
# })

# # Insert post-transform on right side
# insert_into_etl(config, side="right", stage="post", component="transform", data={
#     "lookUpDataSources": [],
#     "transformRuleSet": {
#         "transformRules": [
#             {"name": "Price", "transformers": [
#                 {"transformerType": "string", "args": ["$Price"], "transformer": "TRIM"}
#             ]}
#         ]
#     }
# })
