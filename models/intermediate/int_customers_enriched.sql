WITH main_address_ranked AS (
	SELECT
		customer_id
		, rue 
		, ville 
		, code_postal
		, pays
        , row_number() over(
            partition by customer_id 
            order by address_id) AS rn
	FROM {{ref('stg_addresses')}} 
	WHERE type_address = 'principale'
), 

main_address AS (
    SELECT
        customer_id
        , rue
        , ville
        , code_postal
        , pays
    FROM main_address_ranked
    WHERE rn = 1
),

customer_segments_ranked AS (
	SELECT
        cs.customer_id
        , try_cast(cs.rfm_score AS INTEGER) AS rfm_score
        , cast(cs.segment_label AS VARCHAR) AS segment_label
        , try_cast(cs.total_achats_ht AS DOUBLE) AS total_achats_ht
        , try_cast(cs.nb_commandes AS INTEGER) AS nb_commandes
        , row_number() over(
            partition by cs.customer_id
            order by cs.date_calcul DESC
        ) AS rn
    FROM {{ ref('stg_customers_segments') }} cs
),

customer_segments AS (
    SELECT
        customer_id
        , rfm_score
        , segment_label
        , total_achats_ht
        , nb_commandes
    FROM customer_segments_ranked
    WHERE rn = 1
),

loyalty_accounts_ranked AS (
	SELECT
		customer_id
		, points_solde
		, niveau 
		, row_number() over(
			partition by customer_id 
			order by date_adhesion DESC) AS rn
 	FROM {{ ref("stg_loyalty_accounts")}}
 ), 
 
 loyalty_accounts AS (
 	SELECT * 
 	FROM loyalty_accounts_ranked
 	WHERE rn = 1
 )
 
SELECT
	c.customer_id
	, c.nom
	, c.prenom 
	, c.email 
	, c.telephone
	, ma.rue
	, ma.ville
	, ma.pays
	, c.date_inscription 
	, c.date_naissance 
	, try_cast(c.newsletter AS BOOL) AS newsletter
	, c.segment
	, cs.rfm_score
	, cs.segment_label 
	, cs.total_achats_ht
	, cs.nb_commandes
	, la.points_solde
	, la.niveau
	

FROM {{ref ('stg_customers')}} c 
LEFT JOIN main_address ma ON c.customer_id = ma.customer_id
LEFT JOIN customer_segments cs ON c.customer_id = cs.customer_id
LEFT JOIN loyalty_accounts la ON c.customer_id = la.customer_id