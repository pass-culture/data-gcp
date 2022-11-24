CREATE TABLE past_recommended_offers (
    id             SERIAL PRIMARY KEY,
    userId         int,
    offerId        int,
    date           timestamp with time zone
);

ALTER TABLE past_recommended_offers
ADD COLUMN  group_id       varchar,
ADD COLUMN  reco_origin    varchar,
ADD COLUMN  model_name     varchar,
ADD COLUMN model_version   varchar,
ADD COLUMN call_id         varchar,
ADD COLUMN reco_filters    json,
ADD COLUMN lat varchar,
ADD COLUMN long varchar,
ADD COLUMN user_iris_id    varchar;
