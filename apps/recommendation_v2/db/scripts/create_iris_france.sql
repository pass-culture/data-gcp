CREATE EXTENSION IF NOT EXISTS postgis;
CREATE TABLE iris_france(
    id       varchar,
    irisCode varchar,
    centroid geometry,
    shape    geometry
);

CREATE INDEX idx_iris_france_centroid ON public.iris_france USING gist (centroid);
CREATE INDEX idx_iris_france_shape ON public.iris_france USING gist (shape);
CREATE UNIQUE INDEX iris_france_pkey ON public.iris_france USING btree (id);