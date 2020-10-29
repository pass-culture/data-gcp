CREATE TABLE public.big_offer (
	"idAtProviders" varchar(70) NULL,
	"dateModifiedAtLastProvider" timestamp NULL,
	id bigserial NOT NULL,
	"dateCreated" timestamp NOT NULL,
	"productId" int8 NOT NULL,
	"venueId" int8 NULL,
	"lastProviderId" int8 NULL,
	"bookingEmail" varchar(120) NULL DEFAULT NULL::character varying,
	"isActive" bool NOT NULL DEFAULT true,
	"type" varchar(50) NOT NULL,
	"name" varchar(140) NOT NULL,
	description text NULL,
	conditions varchar(120) NULL,
	"ageMin" int4 NULL,
	"ageMax" int4 NULL,
	url varchar(255) NULL,
	"mediaUrls" _varchar NOT NULL,
	"durationMinutes" int4 NULL,
	"isNational" bool NOT NULL,
	"extraData" json NULL,
	"isDuo" bool NOT NULL DEFAULT false,
	"fieldsUpdated" _varchar NOT NULL DEFAULT '{}'::character varying[],
	"withdrawalDetails" text NULL,
	lastupdate timestamp NULL DEFAULT now(),
	CONSTRAINT "big_offer_idAtProviders_key" UNIQUE ("idAtProviders"),
	CONSTRAINT big_offer_pkey PRIMARY KEY (id),
	CONSTRAINT big_offer_type_check CHECK (((type)::text <> 'None'::text))
);

CREATE INDEX idx_big_offer_fts_name ON public.big_offer USING gin (to_tsvector('french_unaccent'::regconfig, (name)::text));
CREATE INDEX "ix_big_offer_productId" ON public.big_offer USING btree ("productId");
CREATE INDEX ix_big_offer_type ON public.big_offer USING btree (type);
CREATE INDEX "ix_big_offer_venueId" ON public.big_offer USING btree ("venueId");