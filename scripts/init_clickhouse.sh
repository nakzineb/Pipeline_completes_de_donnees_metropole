#!/bin/bash

# =================================================================
# SCRIPT D'INITIALISATION CLICKHOUSE
# =================================================================
# 
# Ce script exécute tous les fichiers SQL pour créer les tables
# dans ClickHouse
# 
# Usage:
#   ./init_clickhouse.sh
# 
# =================================================================

set -e  # Arrêter en cas d'erreur

# Couleurs
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  INITIALISATION CLICKHOUSE${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Vérifier que ClickHouse est accessible
echo -e "${YELLOW}Vérification de ClickHouse...${NC}"
if docker exec clickhouse clickhouse-client --query "SELECT 1" > /dev/null 2>&1; then
    echo -e "${GREEN} ClickHouse est accessible${NC}"
else
    echo -e "${RED} ClickHouse n'est pas accessible${NC}"
    echo -e "${YELLOW}Assurez-vous que le container est démarré:${NC}"
    echo -e "  docker-compose ps clickhouse"
    exit 1
fi

echo ""

# Exécuter les fichiers SQL dans l'ordre
SQL_FILES=(
    "01_raw_tables.sql"
    "02_clean_tables.sql"
    "03_agg_tables.sql"
)

for sql_file in "${SQL_FILES[@]}"; do
    echo -e "${YELLOW} Exécution de $sql_file...${NC}"
    
    if [ -f "clickhouse/init/$sql_file" ]; then
        # Exécuter le fichier SQL
        docker exec -i clickhouse clickhouse-client --multiquery < "clickhouse/init/$sql_file"
        echo -e "${GREEN} $sql_file exécuté avec succès${NC}"
    else
        echo -e "${RED} Fichier $sql_file introuvable${NC}"
        exit 1
    fi
    
    echo ""
done

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN} INITIALISATION TERMINÉE${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""

# Vérifier les tables créées
echo -e "${YELLOW} Tables créées dans velib_db:${NC}"
docker exec clickhouse clickhouse-client --query "SHOW TABLES FROM velib_db"

echo ""
echo -e "${GREEN} ClickHouse est prêt !${NC}"
echo ""
echo -e "${YELLOW}Prochaines étapes:${NC}"
echo -e "  1. Tester l'insertion de données"
echo -e "  2. Créer le consommateur Kafka"
echo -e "  3. Créer le DAG Airflow"