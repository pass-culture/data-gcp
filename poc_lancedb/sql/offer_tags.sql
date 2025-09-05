SELECT tag_name, offer_id
FROM `passculture-data-prod.analytics_prod.global_offer_criterion`
WHERE tag_name IN (
  'programme_anneedelamer25',
  'local_anneedelamer25',
  'numerique_anneedelamer25',
  'livres_anneedelamer25',
  'bergers_film',
  'moonlepanda_film',
  'localart_ecologie25',
  'numeriqueart_ecologie25',
  'biensphysiques_bobdylan25',
  'numerique_bobdylan25',
  'local_tousalopera25',
  'numerique_jea25',
  'livres_jea25',
  'local_jea25',
  'localbresil_ameriquelatine25',
  'numeriquebresil_ameriquelatine25',
  'livresbresil_ameriquelatine25',
  'localmusique_ameriquelatine25',
  'numeriquemusique_ameriquelatine25',
  'albumsmusique_ameriquelatine25',
  'numeriquecuisine_ameriquelatine25',
  'livrescuisine_ameriquelatine25',
  'livresrevolution_ameriquelatine25',
  'numeriquerevolution_ameriquelatine25'
)