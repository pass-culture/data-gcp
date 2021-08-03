CREATE TABLE past_recommended_offers (
    id             SERIAL PRIMARY KEY,
    userId         int,
    offerId        int,
    date           timestamp with time zone
);

ALTER TABLE past_recommended_offers
ADD COLUMN  group_id       varchar,
ADD COLUMN  reco_origin    varchar;
