-- =====================================================================
-- CLICKHOUSE - TABLES RAW (BRONZE LAYER)
-- =====================================================================
-- 
-- Ces tables stockent les données BRUTES provenant de Kafka
-- Aucune transformation, on garde tout tel quel
-- 
-- Utilité :
-- - Backup complet des données sources
-- - Possibilité de "replay" en cas d'erreur dans les transformations
-- - Audit et conformité
-- 
-- =====================================================================

-- Créer la base de données si elle n'existe pas
CREATE DATABASE IF NOT EXISTS velib_db;

USE velib_db;

-- =====================================================================
-- TABLE 1 : raw_station_status
-- =====================================================================
-- Stocke l'état temps réel des stations (disponibilité vélos/bornes)
-- Mise à jour : Chaque minute
-- Volume estimé : ~2M lignes/jour (1500 stations × 1440 minutes)
-- =====================================================================

CREATE TABLE IF NOT EXISTS raw_station_status
(
    -- Identifiants
    station_id String,                    -- ID de la station (ex: "16107")
    
    -- Disponibilité
    num_bikes_available Int32,            -- Nombre total de vélos disponibles
    num_bikes_available_types String,     -- JSON: détail par type (mécanique/électrique)
    num_docks_available Int32,            -- Nombre de bornes libres
    
    -- États
    is_installed Int8,                    -- Station déployée (1) ou non (0)
    is_returning Int8,                    -- Accepte les retours (1/0)
    is_renting Int8,                      -- Permet les locations (1/0)
    
    -- Timestamps
    last_reported Int64,                  -- Timestamp du dernier rapport (epoch)
    ingestion_timestamp Int64,            -- Quand on a ingéré la donnée
    api_last_updated Int64,               -- Quand l'API a mis à jour
    
    -- Métadonnées
    ingestion_date Date DEFAULT toDate(now()),  -- Date d'ingestion (partition)
    
    -- Message Kafka brut (pour debug/replay)
    raw_message String DEFAULT ''         -- Message JSON complet (optionnel)
)
ENGINE = MergeTree()                      -- Moteur de stockage principal de ClickHouse
PARTITION BY toYYYYMM(ingestion_date)     -- Partition par mois (facilite la purge)
ORDER BY (station_id, ingestion_timestamp) -- Index primaire
TTL ingestion_date + INTERVAL 6 MONTH DELETE; -- Suppression auto après 6 mois

-- EXPLICATION DU MOTEUR MergeTree :
-- - Stockage par colonnes (lecture rapide)
-- - Compression automatique (économie disque)
-- - Tri des données selon ORDER BY
-- - Support des partitions (pour purge efficace)

-- EXPLICATION DU TTL (Time To Live) :
-- - Supprime automatiquement les données de plus de 6 mois
-- - Réduit les coûts de stockage
-- - Garde assez d'historique pour analyses annuelles


-- =====================================================================
-- TABLE 2 : raw_station_info
-- =====================================================================
-- Stocke les métadonnées des stations (localisation, capacité, etc.)
-- Mise à jour : Une fois par jour (données quasi-statiques)
-- Volume estimé : ~1500 lignes/jour
-- =====================================================================

CREATE TABLE IF NOT EXISTS raw_station_info
(
    -- Identifiants
    station_id String,                    -- ID de la station
    stationCode String,                   -- Code alternatif de la station
    
    -- Informations géographiques
    name String,                          -- Nom de la station (ex: "Benjamin Godard - Victor Hugo")
    lat Float64,                          -- Latitude (coordonnées GPS)
    lon Float64,                          -- Longitude
    
    -- Capacité
    capacity Int32,                       -- Capacité totale de la station
    
    -- Métadonnées
    rental_methods String,                -- JSON: Méthodes de paiement acceptées
    ingestion_timestamp Int64,            -- Quand on a ingéré
    api_last_updated Int64,               -- Quand l'API a mis à jour
    
    -- Partitionnement
    ingestion_date Date DEFAULT toDate(now()),
    
    -- Message brut
    raw_message String DEFAULT ''
)
ENGINE = ReplacingMergeTree(ingestion_timestamp)  -- Remplace les anciennes versions
PARTITION BY toYYYYMM(ingestion_date)
ORDER BY (station_id, ingestion_timestamp)
TTL ingestion_date + INTERVAL 6 MONTH DELETE;

-- EXPLICATION ReplacingMergeTree :
-- - Même que MergeTree mais garde la dernière version d'une ligne
-- - Basé sur ORDER BY : si station_id identique, garde le plus récent
-- - Parfait pour données qui changent rarement (infos stations)
-- - ClickHouse fait le "remplacement" en arrière-plan (merge)


-- =====================================================================
-- INDEX SECONDAIRES (OPTIONNEL - pour performance)
-- =====================================================================
-- ClickHouse peut créer des index secondaires pour accélérer les requêtes

-- Index sur la date pour raw_station_status
-- ALTER TABLE raw_station_status 
-- ADD INDEX idx_date toDate(fromUnixTimestamp(ingestion_timestamp)) TYPE minmax GRANULARITY 4;

-- Index sur la localisation pour raw_station_info
-- ALTER TABLE raw_station_info 
-- ADD INDEX idx_location (lat, lon) TYPE minmax GRANULARITY 1;

-- =====================================================================
-- VUES UTILES POUR DEBUG
-- =====================================================================

-- Vue : Compter les lignes par jour
CREATE VIEW IF NOT EXISTS v_raw_status_count_by_day AS
SELECT 
    toDate(fromUnixTimestamp(ingestion_timestamp)) as date,
    count() as nb_lignes,
    count(DISTINCT station_id) as nb_stations_distinctes
FROM raw_station_status
GROUP BY date
ORDER BY date DESC;

-- Vue : Dernière mise à jour par station
CREATE VIEW IF NOT EXISTS v_raw_status_latest AS
SELECT 
    station_id,
    max(ingestion_timestamp) as derniere_mise_a_jour,
    fromUnixTimestamp(max(ingestion_timestamp)) as derniere_mise_a_jour_readable
FROM raw_station_status
GROUP BY station_id;

-- =====================================================================
-- REQUÊTES DE TEST
-- =====================================================================

-- Tester l'insertion (à exécuter depuis le consommateur Kafka)
-- INSERT INTO raw_station_status (station_id, num_bikes_available, num_docks_available, is_installed, is_returning, is_renting, last_reported, ingestion_timestamp, api_last_updated, num_bikes_available_types)
-- VALUES ('16107', 12, 15, 1, 1, 1, 1707139200, 1707139250, 1707139200, '[{"mechanical": 8, "ebike": 4}]');

-- Vérifier les données
-- SELECT * FROM raw_station_status LIMIT 10;
-- SELECT * FROM raw_station_info LIMIT 10;

-- Compter les lignes
-- SELECT count() FROM raw_station_status;
-- SELECT count() FROM raw_station_info;

-- Voir les dernières insertions
-- SELECT * FROM raw_station_status ORDER BY ingestion_timestamp DESC LIMIT 10;

-- =====================================================================
-- NOTES IMPORTANTES
-- =====================================================================
-- 
-- 1. PERFORMANCES :
--    - MergeTree est optimisé pour INSERT en masse (batch)
--    - Éviter les INSERT ligne par ligne
--    - Privilégier les batch de 1000-10000 lignes
-- 
-- 2. STOCKAGE :
--    - ClickHouse compresse ~10x les données
--    - 60M lignes/mois ≈ 1.2 GB avec compression
-- 
-- 3. PARTITIONS :
--    - Une partition par mois
--    - Facilite la suppression de vieux data
--    - Améliore les performances sur les requêtes temporelles
-- 
-- 4. ORDER BY :
--    - Définit l'index primaire
--    - Optimise les requêtes filtrant sur ces colonnes
--    - (station_id, timestamp) permet de chercher rapidement par station
-- 
-- 5. TTL :
--    - Suppression automatique après 6 mois
--    - Ajustable selon besoins (1 an, 2 ans, etc.)
--    - Économise de l'espace disque
-- 
-- =====================================================================