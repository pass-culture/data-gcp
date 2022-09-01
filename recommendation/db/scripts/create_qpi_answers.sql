/* Create the function to fetch the non recommendable offers.
 We use a function otherwise the materialized view is a dependency of the tables and blocks the drop operation. */
DROP FUNCTION IF EXISTS get_qpi_answers CASCADE;

CREATE
OR REPLACE FUNCTION get_qpi_answers() RETURNS TABLE (
    user_id varchar,
    "SUPPORT_PHYSIQUE_FILM" bool,
    "ABO_MEDIATHEQUE" bool,
    "VOD" bool,
    "ABO_PLATEFORME_VIDEO" bool,
    "AUTRE_SUPPORT_NUMERIQUE" bool,
    "CARTE_CINE_MULTISEANCES" bool,
    "CARTE_CINE_ILLIMITE" bool,
    "SEANCE_CINE" bool,
    "EVENEMENT_CINE" bool,
    "FESTIVAL_CINE" bool,
    "CINE_VENTE_DISTANCE" bool,
    "CINE_PLEIN_AIR" bool,
    "CONFERENCE" bool,
    "RENCONTRE" bool,
    "DECOUVERTE_METIERS" bool,
    "SALON" bool,
    "CONCOURS" bool,
    "RENCONTRE_JEU" bool,
    "ESCAPE_GAME" bool,
    "EVENEMENT_JEU" bool,
    "JEU_EN_LIGNE" bool,
    "ABO_JEU_VIDEO" bool,
    "ABO_LUDOTHEQUE" bool,
    "LIVRE_PAPIER" bool,
    "LIVRE_NUMERIQUE" bool,
    "TELECHARGEMENT_LIVRE_AUDIO" bool,
    "LIVRE_AUDIO_PHYSIQUE" bool,
    "ABO_BIBLIOTHEQUE" bool,
    "ABO_LIVRE_NUMERIQUE" bool,
    "FESTIVAL_LIVRE" bool,
    "CARTE_MUSEE" bool,
    "ABO_MUSEE" bool,
    "VISITE" bool,
    "VISITE_GUIDEE" bool,
    "EVENEMENT_PATRIMOINE" bool,
    "VISITE_VIRTUELLE" bool,
    "MUSEE_VENTE_DISTANCE" bool,
    "CONCERT" bool,
    "EVENEMENT_MUSIQUE" bool,
    "LIVESTREAM_MUSIQUE" bool,
    "ABO_CONCERT" bool,
    "FESTIVAL_MUSIQUE" bool,
    "SUPPORT_PHYSIQUE_MUSIQUE" bool,
    "TELECHARGEMENT_MUSIQUE" bool,
    "ABO_PLATEFORME_MUSIQUE" bool,
    "CAPTATION_MUSIQUE" bool,
    "SEANCE_ESSAI_PRATIQUE_ART" bool,
    "ATELIER_PRATIQUE_ART" bool,
    "ABO_PRATIQUE_ART" bool,
    "ABO_PRESSE_EN_LIGNE" bool,
    "PODCAST" bool,
    "APP_CULTURELLE" bool,
    "SPECTACLE_REPRESENTATION" bool,
    "SPECTACLE_ENREGISTRE" bool,
    "LIVESTREAM_EVENEMENT" bool,
    "FESTIVAL_SPECTACLE" bool,
    "ABO_SPECTACLE" bool,
    "ACHAT_INSTRUMENT" bool,
    "BON_ACHAT_INSTRUMENT" bool,
    "LOCATION_INSTRUMENT" bool,
    "PARTITION" bool,
    "MATERIEL_ART_CREATIF" bool,
    "ACTIVATION_EVENT" bool,
    "ACTIVATION_THING" bool,
    "JEU_SUPPORT_PHYSIQUE" bool,
    "OEUVRE_ART" bool
) AS $body$ 
BEGIN 
    RETURN QUERY
    SELECT
        *
    FROM
        public.qpi_answers;
END;
$body$ 
LANGUAGE plpgsql;

DROP MATERIALIZED VIEW IF EXISTS qpi_answers_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS qpi_answers_mv AS
SELECT
    *
from
    get_qpi_answers() WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS idx_qpi_answers_mv ON public.qpi_answers_mv USING btree (user_id);

REFRESH MATERIALIZED VIEW qpi_answers_mv;

! -- Creating an index for faster queries.