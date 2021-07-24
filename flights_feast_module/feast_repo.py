from datetime import timedelta

from feast import BigQuerySource, Entity, Feature, FeatureView, ValueType


# Define an entity for the driver. Entities can be thought of as primary keys used to
# retrieve features. Entities are also used to join multiple tables/views during the
# construction of feature vectors
airline = Entity(name="airline", join_key='airline', value_type=ValueType.STRING)
dep_airport = Entity(name="dep_airport", join_key='departure_airport', value_type=ValueType.STRING)
arr_airport = Entity(name="arr_airport", join_key='arrival_airport', value_type=ValueType.STRING)

# Indicates a data source from which feature values can be retrieved. Sources are queried when building training
# datasets or materializing features into an online store.
# event_timestamp is a mandatory field, as below
flight_stats_source = BigQuerySource(
        table_ref="red-beryl-labs.redbq.flight_statistics",
        event_timestamp_column="event_timestamp",
        created_timestamp_column= "created_timestamp",
        )

# Feature views are a grouping based on how features are stored in either the
# online or offline store.
flight_stats_view = FeatureView(
    name="flights_stats",
    entities=["airline","dep_airport","arr_airport"],
    ttl=timedelta(weeks=52),
    features=[
        Feature(name="avg_departure_delay", dtype=ValueType.FLOAT),
        Feature(name="avg_arrival_delay", dtype=ValueType.FLOAT),
    ],
    input=flight_stats_source,
    tags={"team":"flight_performance"},
)





