CREATE TABLE public.target_offer (
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
	CONSTRAINT "target_offer_idAtProviders_key" UNIQUE ("idAtProviders"),
	CONSTRAINT target_offer_pkey PRIMARY KEY (id),
	CONSTRAINT target_offer_type_check CHECK (((type)::text <> 'None'::text))
);

CREATE INDEX idx_target_offer_fts_name ON public.target_offer USING gin (to_tsvector('french_unaccent'::regconfig, (name)::text));
CREATE INDEX "ix_target_offer_productId" ON public.target_offer USING btree ("productId");
CREATE INDEX ix_target_offer_type ON public.target_offer USING btree (type);
CREATE INDEX "ix_target_offer_venueId" ON public.target_offer USING btree ("venueId");