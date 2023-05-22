CREATE TABLE past_similar_offers (
    id              SERIAL PRIMARY KEY,
    user_id         int,
    origin_offer_id int,
    offer_id        int,
    date            timestamp with time zone,
    group_id        varchar,
    model_name      varchar,
    model_version   varchar,
    call_id         varchar,
    reco_filters    json,
    venue_iris_id   varchar
);
