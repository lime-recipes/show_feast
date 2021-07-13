from datetime import timedelta

from feast import BigQuerySource, Entity, Feature, FeatureView, ValueType

# Define an entity for the driver. Entities can be thought of as primary keys used to
# retrieve features. Entities are also used to join multiple tables/views during the
# construction of feature vectors
flight = Entity(
    # Name of the entity. Must be unique within a project
    name="flight_id",
    # The join key of an entity describes the storage level field/column on which
    # features can be looked up. The join key is also used to join feature
    # tables/views when building feature vectors
    join_key="flight_id",
    # The storage level type for an entity
    value_type=ValueType.INT64,
)

# Indicates a data source from which feature values can be retrieved. Sources are queried when building training
# datasets or materializing features into an online store.
flight_stats_source = BigQuerySource(
    # The BigQuery table where features can be found
    table_ref="red-beryl-labs.redbq.airplane_records",
    # The event timestamp is used for point-in-time joins and for ensuring only
    # features within the TTL are returned
    event_timestamp_column="datetime",
    # The (optional) created timestamp is used to ensure there are no duplicate
    # feature rows in the offline store or when building training datasets
    created_timestamp_column="created",
)

# Feature views are a grouping based on how features are stored in either the
# online or offline store.
flight_stats_fv = FeatureView(
    # The unique name of this feature view. Two feature views in a single
    # project cannot have the same name
    name="airplane_records_view",
    # The list of entities specifies the keys required for joining or looking
    # up features from this feature view. The reference provided in this field
    # correspond to the name of a defined entity (or entities)
    entities=["flight_id"],
    # The timedelta is the maximum age that each feature value may have
    # relative to its lookup time. For historical features (used in training),
    # TTL is relative to each timestamp provided in the entity dataframe.
    # TTL also allows for eviction of keys from online stores and limits the
    # amount of historical scanning required for historical feature values
    # during retrieval
    ttl=timedelta(weeks=52),
    # The list of features defined below act as a schema to both define features
    # for both materialization of features into a store, and are used as references
    # during retrieval for building a training dataset or serving features
    features=[
        Feature(name="date", dtype=ValueType.STRING),
        Feature(name="flight_id", dtype=ValueType.FLOAT),
        Feature(name="departure_airport", dtype=ValueType.STRING),
        Feature(name="departure_state", dtype=ValueType.STRING),
        Feature(name="departure_lat", dtype=ValueType.FLOAT),
        Feature(name="departure_lon", dtype=ValueType.FLOAT),
        Feature(name="arrival_airport", dtype=ValueType.STRING),
        Feature(name="arrival_state", dtype=ValueType.STRING),
        Feature(name="arrival_lat", dtype=ValueType.FLOAT),
        Feature(name="arrival_log", dtype=ValueType.FLOAT),
        Feature(name="avg_arrival_delay", dtype=ValueType.FLOAT),
        Feature(name="delayed", dtype=ValueType.INT)
    ],
    # Inputs are used to find feature values. In the case of this feature
    # view we will query a source table on BigQuery for driver statistics
    # features
    input=driver_stats_source,
    # Tags are user defined key/value pairs that are attached to each
    # feature view
    tags={"team": "flight_performance"},
)
