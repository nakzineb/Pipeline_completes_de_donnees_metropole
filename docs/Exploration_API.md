#  Exploration de l'API Vélib' Métropole

##  Résumé de l'API

### Caractéristiques principales
- **Format** : JSON (UTF-8, RFC 4627)
- **Standard** : GBFS 1.0 (General Bikeshare Feed Specification)
- **Authentification** : Aucune clé API requise ! 🎉
- **Fréquence de mise à jour** : Chaque minute
- **Licence** : Open License / Licence Ouverte Etalab
- **Stations** : ~1400 stations sur 55 communes
- **Quota** : 5000 requêtes/jour en mode anonyme

---

## 🔗 URLs des endpoints

### Option 1 : API GBFS officielle (recommandée pour le projet)
```
https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_status.json
https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json
```

### Option 2 : API OpenDataSoft Paris (alternative)
```
https://opendata.paris.fr/api/records/1.0/search/?dataset=velib-disponibilite-en-temps-reel&rows=1500
```

---

##  Structure des données

### 1. **station_status.json** (Données temps réel - DYNAMIQUES)

Contient l'état de disponibilité actuel de chaque station.

```json
{
  "last_updated": 1644321234,  // Timestamp UNIX de la dernière mise à jour
  "ttl": 60,                    // Time To Live en secondes
  "version": "1.0",
  "data": {
    "stations": [
      {
        "station_id": "16107",           // ID unique de la station
        "num_bikes_available": 12,       // Nombre total de vélos disponibles
        "num_bikes_available_types": [   // Détail par type de vélo
          {
            "mechanical": 8,             // Vélos mécaniques
            "ebike": 4                   // Vélos électriques
          }
        ],
        "num_docks_available": 15,       // Nombre de bornes libres
        "is_installed": 1,               // Station déployée (1) ou non (0)
        "is_returning": 1,               // Station accepte les retours (1/0)
        "is_renting": 1,                 // Station permet les locations (1/0)
        "last_reported": 1644321230      // Timestamp du dernier rapport
      }
    ]
  }
}
```

**Champs clés** :
- `station_id` : Identifiant unique (string)
- `num_bikes_available` : Total vélos disponibles (int)
- `num_bikes_available_types` : Détail mécanique/électrique (array)
- `num_docks_available` : Bornes libres (int)
- `is_installed`, `is_renting`, `is_returning` : États binaires (0/1)
- `last_reported` : Horodatage (timestamp UNIX)

---

### 2. **station_information.json** (Métadonnées - STATIQUES)

Contient les informations fixes sur chaque station (localisation, capacité...).

```json
{
  "last_updated": 1644321234,
  "ttl": 86400,
  "version": "1.0",
  "data": {
    "stations": [
      {
        "station_id": "16107",              // ID unique (correspond à station_status)
        "name": "Benjamin Godard - Victor Hugo",  // Nom de la station
        "lat": 48.865983,                   // Latitude
        "lon": 2.275725,                    // Longitude
        "capacity": 35,                     // Capacité totale de la station
        "stationCode": "16107",             // Code station
        "rental_methods": ["CREDITCARD"],   // Méthodes de paiement
        "station_type": "regular"           // Type de station
      }
    ]
  }
}
```

**Champs clés** :
- `station_id` : Même ID que dans station_status (jointure possible!)
- `name` : Nom/adresse de la station (string)
- `lat`, `lon` : Coordonnées GPS (float)
- `capacity` : Capacité totale (int)
- `stationCode` : Code alternatif (string)

---

##  Points importants pour le projet

### Pourquoi deux endpoints ?
- **station_status** → Données **temps réel** qui changent constamment (disponibilité)
- **station_information** → Données **statiques** qui changent rarement (localisation, capacité)

### Jointure des données
Les deux sources se joignent via `station_id` :
```
station_status.station_id = station_information.station_id
```

### Fréquence recommandée de collecte
- **station_status** : Chaque minute (données temps réel)
- **station_information** : Une fois par jour (données quasi-statiques)

---

##  Cas d'usage pour les indicateurs

Avec ces données, on peut calculer :

### Disponibilité
- % de stations vides (num_bikes_available = 0)
- % de stations pleines (num_docks_available = 0)
- Taux d'occupation moyen par station

### Patterns temporels
- Heures de pointe (quand les stations se vident/remplissent)
- Différences weekend vs semaine
- Variations saisonnières

### Analyse géographique
- Zones avec forte demande
- Flux de déplacements (stations sources vs destinations)
- Identification de "stations problématiques"

### Comparaison vélos mécaniques vs électriques
- Préférence des utilisateurs
- Disponibilité relative
- Taux de rotation



---

##  Ressources
- [Documentation GBFS](https://github.com/MobilityData/gbfs)
- [Site officiel Vélib'](https://www.velib-metropole.fr/donnees-open-data-gbfs-du-service-velib-metropole)
- [Portail Open Data Paris](https://opendata.paris.fr)