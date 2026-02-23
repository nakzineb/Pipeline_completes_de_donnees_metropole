#!/bin/bash

# =================================================================
# SCRIPT DE GESTION DU PIPELINE VELIB
# =================================================================
# Usage: ./setup.sh [commande]
# =================================================================

set -e  # Arrêter en cas d'erreur

# Couleurs pour l'affichage
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Fonctions utilitaires
print_success() {
    echo -e "${GREEN} $1${NC}"
}

print_error() {
    echo -e "${RED} $1${NC}"
}

print_info() {
    echo -e "${BLUE}  $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}  $1${NC}"
}

print_header() {
    echo ""
    echo -e "${BLUE}=====================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=====================================${NC}"
    echo ""
}

# Vérifier que Docker est installé
check_docker() {
    if ! command -v docker &> /dev/null; then
        print_error "Docker n'est pas installé"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null && ! docker compose version &> /dev/null; then
        print_error "Docker Compose n'est pas installé"
        exit 1
    fi
    
    print_success "Docker et Docker Compose sont installés"
}

# Créer les dossiers nécessaires
create_directories() {
    print_header "Création des dossiers"
    
    mkdir -p airflow/dags
    mkdir -p airflow/logs
    mkdir -p airflow/plugins
    mkdir -p kafka
    mkdir -p clickhouse/init
    mkdir -p scripts
    mkdir -p docs
    
    # Créer des fichiers .gitkeep pour garder les dossiers vides dans git
    touch airflow/logs/.gitkeep
    
    print_success "Dossiers créés"
}

# Démarrer tous les services
start_services() {
    print_header "Démarrage des services"
    
    docker-compose up -d
    
    print_success "Services démarrés"
    print_info "Attendez quelques secondes que tous les services soient prêts..."
    
    sleep 10
    
    show_status
}

# Arrêter tous les services
stop_services() {
    print_header "Arrêt des services"
    
    docker-compose down
    
    print_success "Services arrêtés"
}

# Redémarrer tous les services
restart_services() {
    print_header "Redémarrage des services"
    
    docker-compose restart
    
    print_success "Services redémarrés"
}

# Afficher le status des services
show_status() {
    print_header "Status des services"
    
    docker-compose ps
    
    echo ""
    print_info "Accès aux services:"
    echo "   Airflow UI : http://localhost:8080 (admin/admin)"
    echo "   ClickHouse : http://localhost:8123"
    echo "   PostgreSQL : localhost:5432"
    echo "   Kafka : localhost:9092"
}

# Afficher les logs
show_logs() {
    if [ -z "$1" ]; then
        print_info "Affichage de tous les logs (Ctrl+C pour arrêter)"
        docker-compose logs -f
    else
        print_info "Affichage des logs de $1 (Ctrl+C pour arrêter)"
        docker-compose logs -f "$1"
    fi
}

# Nettoyer tout (ATTENTION : supprime les données !)
clean_all() {
    print_warning "ATTENTION : Cette action va supprimer TOUTES les données !"
    read -p "Êtes-vous sûr ? (y/N) " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_header "Nettoyage complet"
        
        docker-compose down -v
        
        print_success "Nettoyage terminé"
    else
        print_info "Annulé"
    fi
}

# Tester la connexion à l'API Vélib'
test_api() {
    print_header "Test de l'API Vélib'"
    
    python3 scripts/test_api.py 2>/dev/null || print_error "Le script test_api.py n'existe pas encore"
}

# Créer les topics Kafka
create_kafka_topics() {
    print_header "Création des topics Kafka"
    
    # Attendre que Kafka soit prêt
    sleep 5
    
    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --topic velib-station-status \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists
    
    docker exec kafka kafka-topics --create \
        --bootstrap-server localhost:9092 \
        --topic velib-station-info \
        --partitions 1 \
        --replication-factor 1 \
        --if-not-exists
    
    print_success "Topics Kafka créés"
    
    # Lister les topics
    print_info "Topics disponibles:"
    docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
}

# Initialiser le projet complet
init_project() {
    print_header " INITIALISATION DU PROJET VELIB PIPELINE"
    
    check_docker
    create_directories
    start_services
    
    print_info "Attente que les services soient complètement prêts (30s)..."
    sleep 30
    
    create_kafka_topics
    
    print_header " PROJET INITIALISÉ AVEC SUCCÈS !"
    
    print_info "Prochaines étapes:"
    echo "  1. Vérifier l'interface Airflow : http://localhost:8080"
    echo "  2. Créer les producteurs et consommateurs Kafka"
    echo "  3. Créer les DAGs Airflow"
    echo "  4. Créer les schémas ClickHouse"
}

# Afficher l'aide
show_help() {
    echo "Usage: ./setup.sh [commande]"
    echo ""
    echo "Commandes disponibles:"
    echo "  init          Initialiser le projet complet (première fois)"
    echo "  start         Démarrer tous les services"
    echo "  stop          Arrêter tous les services"
    echo "  restart       Redémarrer tous les services"
    echo "  status        Afficher le status des services"
    echo "  logs [svc]    Afficher les logs (optionnel: service spécifique)"
    echo "  clean         Nettoyer tout (supprime les données !)"
    echo "  topics        Créer les topics Kafka"
    echo "  test-api      Tester l'API Vélib'"
    echo "  help          Afficher cette aide"
    echo ""
    echo "Exemples:"
    echo "  ./setup.sh init                  # Première initialisation"
    echo "  ./setup.sh logs airflow-webserver  # Voir les logs d'Airflow"
    echo "  ./setup.sh status                # Voir l'état des services"
}

# =================================================================
# MAIN
# =================================================================

case "$1" in
    init)
        init_project
        ;;
    start)
        start_services
        ;;
    stop)
        stop_services
        ;;
    restart)
        restart_services
        ;;
    status)
        show_status
        ;;
    logs)
        show_logs "$2"
        ;;
    clean)
        clean_all
        ;;
    topics)
        create_kafka_topics
        ;;
    test-api)
        test_api
        ;;
    help|--help|-h|"")
        show_help
        ;;
    *)
        print_error "Commande inconnue: $1"
        show_help
        exit 1
        ;;
esac