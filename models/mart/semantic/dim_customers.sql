with source as (
    select * from {{ ref('int_customers_enriched') }}
)

select
    customer_id
   ,  prenom
   ,  nom
   ,  email
   ,  telephone
   ,  rue
   ,  ville
   ,  pays
   ,  date_naissance
   ,  date_inscription
   ,  newsletter
   ,  segment                                         as segment_commercial
   ,  segment_label                                   as segment_rfm
   ,  rfm_score
   ,  niveau                                          as niveau_loyaute
    -- Statut achat (calculé depuis int)
   ,  case
        when nb_commandes is null
          or nb_commandes = 0  then 'Inactif'
        when nb_commandes = 1  then 'Nouveau'
        when nb_commandes <= 4 then 'Régulier'
        else 'Fidèle'
    end                                             as statut_achat
    -- Ancienneté
   ,  datediff('day', date_inscription, current_date) as anciennete_jours
   ,  date_inscription >= current_date - interval '90 days'
                                                    as is_nouveau_client

from source