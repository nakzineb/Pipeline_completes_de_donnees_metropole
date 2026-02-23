-- =====================================================================
-- CLICKHOUSE - TABLES CLEAN (SILVER LAYER)
-- =====================================================================
-- 
-- Ces tables contiennent les données NETTOYÉES et TRANSFORMÉES
-- Prêtes pour l'analyse et les dashboards
-- 
-- Différences avec RAW :
-- - Données validées (pas de NULL, types corrects)
-- - JSON parsé en colonnes
-- - Jointures possibles entre dimensions et faits
-- - Optimisées pour les requêtes analytiques
-- 
-- =====================================================================

USE velib_db;

-- =====================================================================
-- DIMENSION : dim_stations
-- =====================================================================
-- Table de dimension contenant les informations des stations
-- Type : Slowly Changing Dimension (SCD) Type 2
-- 
-- SCD Type 2 = on garde l'historique des changements
-- Exemple : Si une station change de nom ou capacité, on ajoute une ligne
-- =====================================================================

CREATE TABLE IF NOT EXISTS dim_stations
(
    -- Clé surrogate (artificielle)
    station_key Int64,                    -- Clé unique auto-incrémentée
    
    -- Clé naturelle (de la source)
    station_id String,                    -- ID Vélib de la station
    stationCode String,                   -- Code alternatif
    
    -- Attributs de la station
    name String,                          -- Nom de la station
    lat Float64,                          -- Latitude
    lon Float64,                          -- Longitude
    capacity Int32,                       -- Capacité totale
    
    -- Méthodes de paiement (parsé du JSON)
    has_creditcard UInt8,                 -- Accepte carte de crédit (1/0)
    
    -- Géolocalisation enrichie (calculée)
    arrondissement Nullable(String),      -- Déduit de lat/lon (à calculer)
    quartier Nullable(String),            -- Quartier (à enrichir)
    
    -- SCD Type 2 : Gestion de l'historique
    valid_from DateTime DEFAULT now(),    -- Date de début de validité
    valid_to DateTime DEFAULT toDateTime('2099-12-31'),  -- Date de fin (2099 = actif)
    is_current UInt8 DEFAULT 1,           -- 1 = ligne actuelle, 0 = historique
    
    -- Métadonnées
    created_at DateTime DEFAULT now(),
    updated_at DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(updated_at)
ORDER BY (station_id, valid_from)
PARTITION BY toYYYYMM(valid_from);

-- EXPLICATION SCD TYPE 2 :
-- 
-- Exemple : Station "16107" change de nom
-- 
-- Ligne 1 : station_id=16107, name="Vieux nom",  valid_from=2024-01-01, valid_to=2024-06-15, is_current=0
-- Ligne 2 : station_id=16107, name="Nouveau nom", valid_from=2024-06-16, valid_to=2099-12-31, is_current=1
-- 
-- Avantage : On peut analyser les données historiques avec le bon nom


-- =====================================================================
-- FAIT : fact_station_availability
-- =====================================================================
-- Table de faits contenant les mesures de disponibilité
-- Granularité : Une ligne par station par minute
-- =====================================================================

CREATE TABLE IF NOT EXISTS fact_station_availability
(
    -- Clés
    station_id String,                    -- Clé étrangère vers dim_stations
    
    -- Dimensions temporelles
    measurement_time DateTime,            -- Timestamp de la mesure
    measurement_date Date,                -- Date (pour partition)
    hour UInt8,                           -- Heure (0-23)
    day_of_week UInt8,                    -- Jour de la semaine (1=Lundi, 7=Dimanche)
    is_weekend UInt8,                     -- 1 si weekend, 0 sinon
    
    -- Mesures : Vélos
    num_bikes_available Int32,            -- Total vélos disponibles
    num_bikes_mechanical Int32,           -- Vélos mécaniques
    num_bikes_electric Int32,             -- Vélos électriques
    
    -- Mesures : Bornes
    num_docks_available Int32,            -- Bornes libres
    num_docks_total Int32,                -- Total bornes (capacity from dim)
    
    -- Mesures calculées
    occupancy_rate Float32,               -- Taux d'occupation (bikes / capacity)
    availability_rate Float32,            -- Taux de disponibilité (docks / capacity)
    
    -- États
    is_installed UInt8,                   -- Station active
    is_renting UInt8,                     -- Permet locations
    is_returning UInt8,                   -- Accepte retours
    is_operational UInt8,                 -- Opérationnelle (renting AND returning)
    
    -- Indicateurs de qualité
    is_empty UInt8,                       -- Station vide (0 vélos)
    is_full UInt8,                        -- Station pleine (0 bornes)
    is_critical UInt8,                    -- Critique (< 20% disponibilité)
    
    -- Métadonnées
    last_reported Int64,                  -- Timestamp source
    ingestion_timestamp Int64             -- Timestamp ingestion
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(measurement_date)
ORDER BY (station_id, measurement_time)
TTL measurement_date + INTERVAL 6 MONTH DELETE;

-- EXPLICATION FACT TABLE :
-- - Contient les MESURES (metrics) pas les attributs
-- - Très volumineuse (2M lignes/jour)
-- - Optimisée pour agrégations (SUM, AVG, COUNT)
-- - Join avec dim_stations pour avoir le nom de la station


-- =====================================================================
-- MATERIALIZED VIEW : Transformation automatique RAW → CLEAN
-- =====================================================================
-- 
-- Ces vues matérialisées transforment automatiquement les données
-- quand elles arrivent dans les tables RAW
-- 
-- Avantage : Temps réel ! Pas besoin de batch ETL
-- =====================================================================

-- Vue matérialisée : RAW status → FACT availability
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_raw_to_fact
TO fact_station_availability
AS
SELECT
    -- Clés
    station_id,
    
    -- Dimensions temporelles
    fromUnixTimestamp(last_reported) as measurement_time,
    toDate(fromUnixTimestamp(last_reported)) as measurement_date,
    toHour(fromUnixTimestamp(last_reported)) as hour,
    toDayOfWeek(fromUnixTimestamp(last_reported)) as day_of_week,
    if(toDayOfWeek(fromUnixTimestamp(last_reported)) IN (6, 7), 1, 0) as is_weekend,
    
    -- Mesures : Vélos
    num_bikes_available,
    
    -- Parser le JSON pour extraire mechanical et ebike
    toInt32OrZero(JSONExtractString(num_bikes_available_types, 'mechanical')) as num_bikes_mechanical,
    toInt32OrZero(JSONExtractString(num_bikes_available_types, 'ebike')) as num_bikes_electric,
    
    -- Mesures : Bornes
    num_docks_available,
    num_bikes_available + num_docks_available as num_docks_total,  -- Approximation
    
    -- Mesures calculées
    if(num_bikes_available + num_docks_available > 0,
       num_bikes_available / (num_bikes_available + num_docks_available),
       0) as occupancy_rate,
    if(num_bikes_available + num_docks_available > 0,
       num_docks_available / (num_bikes_available + num_docks_available),
       0) as availability_rate,
    
    -- États
    is_installed,
    is_renting,
    is_returning,
    if(is_renting = 1 AND is_returning = 1, 1, 0) as is_operational,
    
    -- Indicateurs
    if(num_bikes_available = 0, 1, 0) as is_empty,
    if(num_docks_available = 0, 1, 0) as is_full,
    if(num_docks_available + num_bikes_available > 0 AND
       num_docks_available / (num_docks_available + num_bikes_available) < 0.2,
       1, 0) as is_critical,
    
    -- Métadonnées
    last_reported,
    ingestion_timestamp
    
FROM raw_station_status;

-- EXPLICATION MATERIALIZED VIEW :
-- - S'exécute automatiquement à chaque INSERT dans raw_station_status
-- - Transforme et insère dans fact_station_availability
-- - Temps réel : pas de batch ETL nécessaire !
-- - Économie : on transforme une seule fois, pas à chaque requête


-- =====================================================================
-- REQUÊTES D'EXEMPLE SUR LES TABLES CLEAN
-- =====================================================================

-- Exemple 1 : Disponibilité actuelle de toutes les stations
-- SELECT 
--     s.name,
--     f.num_bikes_available,
--     f.num_docks_available,
--     round(f.occupancy_rate * 100, 1) as taux_occupation_pct
-- FROM fact_station_availability f
-- JOIN dim_stations s ON f.station_id = s.station_id AND s.is_current = 1
-- WHERE f.measurement_time > now() - INTERVAL 5 MINUTE
-- ORDER BY f.measurement_time DESC
-- LIMIT 20;

-- Exemple 2 : Stations actuellement vides
-- SELECT 
--     s.name,
--     f.measurement_time,
--     f.num_bikes_available,
--     f.num_docks_available
-- FROM fact_station_availability f
-- JOIN dim_stations s ON f.station_id = s.station_id AND s.is_current = 1
-- WHERE f.is_empty = 1
--   AND f.measurement_time > now() - INTERVAL 10 MINUTE;

-- Exemple 3 : Taux moyen d'occupation par heure de la journée
-- SELECT 
--     hour,
--     round(avg(occupancy_rate) * 100, 1) as taux_occupation_moyen_pct,
--     count() as nb_mesures
-- FROM fact_station_availability
-- WHERE measurement_date >= today() - 7
-- GROUP BY hour
-- ORDER BY hour;

-- =====================================================================
-- NOTES IMPORTANTES
-- =====================================================================
-- 
-- 1. MODÈLE EN ÉTOILE :
--    - fact_station_availability = table de faits (centre)
--    - dim_stations = dimension (branche)
--    - Facilite les requêtes analytiques
-- 
-- 2. MATERIALIZED VIEW :
--    - Transformation automatique en temps réel
--    - Pas besoin de batch ETL
--    - Coût : espace disque (stocké 2 fois : raw + clean)
-- 
-- 3. SCD TYPE 2 :
--    - Permet l'analyse historique correcte
--    - Attention aux JOINs : toujours filtrer is_current = 1
--    - Ou utiliser valid_from/valid_to pour analyses temporelles
-- 
-- 4. PERFORMANCES :
--    - Les mesures calculées (occupancy_rate) sont PRÉ-CALCULÉES
--    - Pas de calcul à chaque requête
--    - Trade-off : espace vs vitesse
-- 
-- =====================================================================