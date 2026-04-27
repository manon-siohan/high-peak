WITH source AS (
    SELECT * FROM {{ ref('stg_stores')}}
)


SELECT
    store_id
  ,  store_name
  ,  store_ville
  ,  store_region
  ,  store_pays
  ,  type                                            AS type_magasin
  ,  surface_m2
  ,  date_ouverture
  ,  date_fermeture
  ,  actif
  ,  actif = true                                    AS is_active
  ,  type = 'e-commerce'                             AS is_online
    -- Regroupement géographique
  ,  CASE
        WHEN store_pays = 'France' THEN 'France'
        ELSE 'International'
    END                                             AS zone_geo

FROM source
--WHERE actif = true
