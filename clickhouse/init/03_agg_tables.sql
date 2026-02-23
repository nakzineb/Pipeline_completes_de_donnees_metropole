-- =====================================================================
-- CLICKHOUSE - TABLES AGG (GOLD LAYER)
-- =====================================================================
-- 
-- Ces tables contiennent des données PRÉ-AGRÉGÉES
-- Ultra-rapides pour les dashboards et rapports
-- 
-- Avantage :
-- - Requêtes instantanées (pas de calcul à la volée)
-- - Moins de données à scanner
-- - Parfait pour les dashboards temps réel
-- 
-- Trade-off :
-- - Espace disque (mais ~10x moins que les faits bruts)
-- - Flexibilité (agrégations figées)
-- 
-- =====================================================================

USE velib_db;

-- =====================================================================
-- AGG 1 : agg_hourly_stats
-- =====================================================================
-- Statistiques par station par heure
-- Volume : ~35k lignes/jour (1500 stations × 24 heures)
-- Réduction : 98% vs données brutes (2M → 35k lignes)
-- =====================================================================

CREATE TABLE IF NOT EXISTS agg_hourly_stats
(
    -- Dimensions
    station_id String,
    hour_timestamp DateTime,              -- Début de l'heure (ex: 2024-01-15 14:00:00)
    date Date,
    hour UInt8,
    day_of_week UInt8,
    is_weekend UInt8,
    
    -- Agrégations : Vélos disponibles
    avg_bikes_available Float32,          -- Moyenne sur l'heure
    min_bikes_available Int32,            -- Minimum
    max_bikes_available Int32,            -- Maximum
    median_bikes_available Float32,       -- Médiane
    
    avg_bikes_mechanical Float32,
    avg_bikes_electric Float32,
    
    -- Agrégations : Bornes disponibles
    avg_docks_available Float32,
    min_docks_available Int32,
    max_docks_available Int32,
    
    -- Agrégations : Taux
    avg_occupancy_rate Float32,           -- Taux moyen d'occupation
    avg_availability_rate Float32,        -- Taux moyen de disponibilité
    
    -- Compteurs d'états
    minutes_empty UInt32,                 -- Nombre de minutes vides
    minutes_full UInt32,                  -- Nombre de minutes pleines
    minutes_critical UInt32,              -- Nombre de minutes critiques
    minutes_operational UInt32,           -- Nombre de minutes opérationnelles
    
    -- Statistiques de mesures
    nb_measurements UInt32,               -- Nombre de mesures dans l'heure
    first_measurement DateTime,           -- Première mesure
    last_measurement DateTime,            -- Dernière mesure
    
    -- Métadonnées
    computed_at DateTime DEFAULT now()
)
ENGINE = SummingMergeTree()               -- Optimisé pour les agrégations
PARTITION BY toYYYYMM(date)
ORDER BY (station_id, hour_timestamp);

-- EXPLICATION SummingMergeTree :
-- - Additionne automatiquement les colonnes numériques
-- - Parfait pour les agrégations pré-calculées
-- - Économise de l'espace en fusionnant les lignes


-- =====================================================================
-- AGG 2 : agg_daily_stats
-- =====================================================================
-- Statistiques par station par jour
-- Volume : ~1.5k lignes/jour (1500 stations)
-- Réduction : 99.9% vs données brutes (2M → 1.5k lignes)
-- =====================================================================

CREATE TABLE IF NOT EXISTS agg_daily_stats
(
    -- Dimensions
    station_id String,
    date Date,
    day_of_week UInt8,
    is_weekend UInt8,
    
    -- Agrégations globales
    avg_bikes_available Float32,
    min_bikes_available Int32,
    max_bikes_available Int32,
    
    avg_bikes_mechanical Float32,
    avg_bikes_electric Float32,
    
    avg_docks_available Float32,
    
    avg_occupancy_rate Float32,
    avg_availability_rate Float32,
    
    -- Pics journaliers
    peak_hour_morning UInt8,              -- Heure de pointe matin (max occupation)
    peak_bikes_morning Int32,             -- Vélos au pic du matin
    
    peak_hour_evening UInt8,              -- Heure de pointe soir
    peak_bikes_evening Int32,             -- Vélos au pic du soir
    
    -- Durées d'états (en heures)
    hours_empty Float32,                  -- Heures passées vide
    hours_full Float32,                   -- Heures passées plein
    hours_critical Float32,               -- Heures en état critique
    hours_operational Float32,            -- Heures opérationnelles
    
    -- Statistiques de rotation
    estimated_rotations UInt32,           -- Estimation du nb de rotations
    
    -- Statistiques de mesures
    nb_measurements UInt32,
    completeness_rate Float32,            -- Taux de complétude (nb_measurements / 1440)
    
    -- Métadonnées
    computed_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(computed_at)
PARTITION BY toYYYYMM(date)
ORDER BY (station_id, date);


-- =====================================================================
-- AGG 3 : agg_station_summary
-- =====================================================================
-- Résumé global par station (sur toute la période)
-- Volume : ~1.5k lignes (1 par station)
-- Mise à jour : Journalière
-- =====================================================================

CREATE TABLE IF NOT EXISTS agg_station_summary
(
    -- Clé
    station_id String,
    station_name String,                  -- Denormalisé pour performance
    
    -- Statistiques globales (tous les temps)
    avg_bikes_available_alltime Float32,
    avg_occupancy_rate_alltime Float32,
    
    -- Statistiques par période
    avg_bikes_weekday Float32,            -- Moyenne en semaine
    avg_bikes_weekend Float32,            -- Moyenne weekend
    
    -- Popularité
    popularity_score Float32,             -- Score de popularité (0-100)
    is_hotspot UInt8,                     -- Station très fréquentée (1/0)
    
    -- Problèmes identifiés
    days_empty_last_month UInt32,         -- Jours vides le mois dernier
    days_full_last_month UInt32,          -- Jours pleins le mois dernier
    avg_operational_rate Float32,         -- % du temps opérationnel
    
    -- Patterns temporels
    best_hour_to_take_bike UInt8,         -- Meilleure heure pour prendre un vélo
    best_hour_to_return_bike UInt8,       -- Meilleure heure pour rendre un vélo
    
    -- Métriques de fiabilité
    reliability_score Float32,            -- Score de fiabilité (0-100)
    data_quality_score Float32,           -- Score qualité données (0-100)
    
    -- Métadonnées
    first_seen Date,                      -- Première apparition
    last_seen Date,                       -- Dernière apparition
    nb_days_active UInt32,                -- Nombre de jours actifs
    
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY station_id;


-- =====================================================================
-- MATERIALIZED VIEWS : Calcul automatique des agrégations
-- =====================================================================

-- Vue matérialisée : FACT → AGG hourly
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_fact_to_hourly
TO agg_hourly_stats
AS
SELECT
    station_id,
    toStartOfHour(measurement_time) as hour_timestamp,
    toDate(measurement_time) as date,
    toHour(measurement_time) as hour,
    toDayOfWeek(measurement_time) as day_of_week,
    if(toDayOfWeek(measurement_time) IN (6, 7), 1, 0) as is_weekend,
    
    -- Moyennes
    avg(num_bikes_available) as avg_bikes_available,
    min(num_bikes_available) as min_bikes_available,
    max(num_bikes_available) as max_bikes_available,
    quantile(0.5)(num_bikes_available) as median_bikes_available,
    
    avg(num_bikes_mechanical) as avg_bikes_mechanical,
    avg(num_bikes_electric) as avg_bikes_electric,
    
    avg(num_docks_available) as avg_docks_available,
    min(num_docks_available) as min_docks_available,
    max(num_docks_available) as max_docks_available,
    
    avg(occupancy_rate) as avg_occupancy_rate,
    avg(availability_rate) as avg_availability_rate,
    
    -- Compteurs
    sum(is_empty) as minutes_empty,
    sum(is_full) as minutes_full,
    sum(is_critical) as minutes_critical,
    sum(is_operational) as minutes_operational,
    
    -- Statistiques
    count() as nb_measurements,
    min(measurement_time) as first_measurement,
    max(measurement_time) as last_measurement,
    
    now() as computed_at
    
FROM fact_station_availability
GROUP BY 
    station_id,
    toStartOfHour(measurement_time),
    toDate(measurement_time),
    toHour(measurement_time),
    toDayOfWeek(measurement_time),
    is_weekend;

-- EXPLICATION :
-- - Cette vue s'exécute automatiquement quand des données arrivent
-- - Calcule les stats horaires en temps réel
-- - Évite de recalculer à chaque requête


-- =====================================================================
-- REQUÊTES D'EXEMPLE SUR LES TABLES AGG
-- =====================================================================

-- Exemple 1 : Stations les plus saturées aujourd'hui
-- SELECT 
--     station_id,
--     avg_occupancy_rate,
--     hours_full,
--     hours_critical
-- FROM agg_daily_stats
-- WHERE date = today()
-- ORDER BY hours_full DESC
-- LIMIT 10;

-- Exemple 2 : Pattern d'utilisation par heure (moyenne sur 7 jours)
-- SELECT 
--     hour,
--     avg(avg_bikes_available) as avg_bikes,
--     avg(avg_occupancy_rate) * 100 as taux_occupation_pct
-- FROM agg_hourly_stats
-- WHERE date >= today() - 7
-- GROUP BY hour
-- ORDER BY hour;

-- Exemple 3 : Top 10 stations les plus populaires
-- SELECT 
--     station_name,
--     popularity_score,
--     avg_bikes_weekday,
--     avg_bikes_weekend
-- FROM agg_station_summary
-- WHERE is_hotspot = 1
-- ORDER BY popularity_score DESC
-- LIMIT 10;

-- Exemple 4 : Stations avec problèmes récurrents
-- SELECT 
--     station_name,
--     days_empty_last_month,
--     days_full_last_month,
--     avg_operational_rate * 100 as taux_operational_pct,
--     reliability_score
-- FROM agg_station_summary
-- WHERE reliability_score < 70
-- ORDER BY reliability_score ASC
-- LIMIT 10;

-- =====================================================================
-- MAINTENANCE : Rafraîchir les agrégations
-- =====================================================================

-- Les vues matérialisées se mettent à jour automatiquement
-- Mais pour agg_station_summary, on peut avoir besoin d'un refresh manuel

-- Procédure de refresh (à exécuter via un DAG Airflow quotidien)
-- 
-- TRUNCATE TABLE agg_station_summary;
-- 
-- INSERT INTO agg_station_summary
-- SELECT 
--     f.station_id,
--     any(s.name) as station_name,
--     avg(f.num_bikes_available) as avg_bikes_available_alltime,
--     avg(f.occupancy_rate) as avg_occupancy_rate_alltime,
--     -- etc...
-- FROM fact_station_availability f
-- LEFT JOIN dim_stations s ON f.station_id = s.station_id AND s.is_current = 1
-- WHERE f.measurement_date >= today() - 30  -- 30 derniers jours
-- GROUP BY f.station_id;

-- =====================================================================
-- NOTES IMPORTANTES
-- =====================================================================
-- 
-- 1. PRÉ-AGRÉGATION :
--    - Calcul fait une fois, utilisé mille fois
--    - Dashboards ultra-rapides
--    - Trade-off : flexibilité vs performance
-- 
-- 2. GRANULARITÉS :
--    - Horaire : Pour patterns intra-journaliers
--    - Journalière : Pour tendances hebdomadaires/mensuelles
--    - Globale : Pour KPIs et résumés
-- 
-- 3. MATERIALIZED VIEWS :
--    - Automatiques pour hourly/daily
--    - Manuel pour summary (via Airflow DAG)
-- 
-- 4. PERFORMANCES :
--    - agg_hourly_stats : ~35k lignes/jour → requêtes < 100ms
--    - agg_daily_stats : ~1.5k lignes/jour → requêtes < 10ms
--    - agg_station_summary : ~1.5k lignes → requêtes < 1ms
-- 
-- 5. ESPACE DISQUE :
--    - RAW : 100%
--    - CLEAN : +50% (à cause des colonnes calculées)
--    - AGG : +2% (très compact)
--    - Total : ~150% de RAW (acceptable)
-- 
-- =====================================================================