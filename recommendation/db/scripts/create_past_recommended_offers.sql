CREATE TABLE past_recommended_offers (
    id             SERIAL PRIMARY KEY,
    userId         int,
    offerId        int,
    date           timestamp with time zone
);
