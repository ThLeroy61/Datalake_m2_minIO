# Lab 1 — Construire un data lake pour le réseau Vélo STAR (Rennes Métropole)

**Module :** Data Lake — M2 Data — Sup de Vinci
**Durée indicative :** ~3h30 (installation comprise) 
**Pré-requis :** un ordinateur Windows, Mac ou Linux, une connexion internet, des droits administrateur 
**Données utilisées :** données **réelles et ouvertes** du réseau de vélos en libre-service Vélo STAR de Rennes Métropole (opérateur Keolis), publiées au format international **GBFS** (General Bikeshare Feed Specification), le standard utilisé par la quasi-totalité des opérateurs de vélos/trottinettes en libre-service dans le monde (et donc par les applications comme Google Maps ou Citymapper pour afficher les vélos disponibles près de vous).
**Ce Lab contient 2 TPs** 

> 🎓 **Le TP 1 n'est pas un exercice « jouet ».** vous allez manipuler exactement le type de flux de données qu'un data engineer manipule réellement dans une entreprise de transport : un flux temps réel standardisé, avec ses imperfections (parfois lent, parfois temporairement indisponible), qu'il faut apprendre à capter, stocker durablement et fiabiliser — car aujourd'hui, cette donnée n'existe qu'à l'instant présent : personne, chez l'opérateur, ne garde un historique exploitable de l'état du réseau. C'est exactement ce vide que vous allez combler.

---

## 0. Mise en situation — comprendre *la problématique avant l'informatique*

Lisez ce paragraphe avant de toucher à quoi que ce soit. Si vous ne comprenez pas le problème métier, vous allez exécuter des commandes sans savoir pourquoi — et vous les oublierez la semaine prochaine.

### Le contexte de l'entreprise

Vous êtes en prestation de service en tant que **data engineer** chez **Keolis Rennes**, l'exploitant du réseau de transport **STAR** de Rennes Métropole (bus, métro, vélos en libre-service « Vélo STAR »). La direction Mobilités a un problème concret :

> *« Aujourd'hui, on sait à tout instant combien de vélos sont disponibles dans chaque station, grâce à notre API temps réel. Mais dès que la minute suivante arrive, l'information d'avant est perdue — elle n'est stockée nulle part. On ne peut donc répondre à aucune question du type : "quelles stations sont souvent vides le matin ?", "à quelle heure faut-il faire passer les camions de rééquilibrage ?", "quelles stations mériteraient plus de bornes ?". On voudrait un premier prototype, pas cher et rapide à mettre en place, qui capture cet état toutes les quelques minutes et le garde en mémoire durablement, pour qu'un(e) data analyst puisse ensuite l'exploiter. »*

C'est un besoin **100% réel** dans le secteur du transport : ce type de projet s'appelle de l'**analyse de saturation** et sert de brique de base à des cas d'usage bien plus avancés (prédiction de saturation par machine learning, optimisation des tournées de rééquilibrage des vélos). Dans ce le Lab vous allez construire **la toute première brique indispensable : le data lake qui capture et conserve la donnée.** Sans cette brique, aucune analyse n'est possible, aussi intelligente soit-elle.

### Ce que vous allez livrer à la fin du Lab

1. Un pipeline automatisé qui va chercher, toutes les quelques minutes, l'état de **toutes les stations Vélo STAR** (vélos disponibles, places libres, station ouverte ou fermée) et le dépose dans un data lake, au format Parquet.
2. Une preuve, dans un notebook Jupyter, que l'historique s'accumule bien dans le temps (chose impossible avec l'API seule).
3. Un enrichissement de cette donnée « brute » avec le référentiel des stations (nom, localisation, capacité) — chargé, lui, manuellement, car il change rarement.
4. Une première exploitation simple : identifier les stations les plus souvent vides ou pleines.

### Voici l'architecture du flux de données que vous allez construire

```
☁️ API GBFS Vélo STAR  →  🧭 Apache Airflow  →  🪣 MinIO (data lake)  →  📓 Jupyter (analyse)
   (source réelle)         (orchestrateur)     (Espace de stockage)     (Moteur de traitement)
```

Ceci est l'architecture de référence de tout projet data Lake. Votre première action sera de produire un cartographie graphique visuelle du flux décrit plus haut.

---

## Sommaire

1. Comprendre la donnée AVANT de coder (obligatoire, ne sautez pas cette étape)
2. Installer les outils
3. Créer le projet
4. Démarrer la pile
5. Le pipeline d'ingestion Vélo STAR
6. Déclencher et vérifier
7. Explorer l'historique dans Jupyter
8. Chargement manuel du référentiel des stations (donnée de dimension)
9. Croiser les données et répondre à une vraie question métier
10. Dépannage
11. Pour aller plus loin
12. Lexique de secours

---

## 1. Comprendre la donnée AVANT de coder

C'est l'étape que les data engineers débutants sautent — et c'est l'erreur n°1. **On n'écrit jamais de code contre une API qu'on n'a pas d'abord regardée avec ses propres yeux.**

### 1.1 Le standard GBFS, en une phrase

**GBFS** (*General Bikeshare Feed Specification*) est un format standardisé, utilisé dans le monde entier, pour publier en temps réel l'état d'un réseau de vélos/trottinettes en libre-service. C'est ce standard que consultent Google Maps ou Citymapper pour t'afficher « 4 vélos disponibles » sur une carte. Documentation officielle (pour information, pas à lire en entier) : `https://github.com/MobilityData/gbfs`.

**Pourquoi c'est important pour vous** : comme c'est un standard, la même méthode que vous allez apprendre aujourd'hui avec Rennes fonctionnera demain avec Paris, Lyon, New York ou n'importe quelle ville équipée de vélos en libre-service. Vous n'apprenez pas « l'API de Rennes », vous apprenez **une compétence générique**.

### 1.2 Le principe de « découverte » d'un flux GBFS (très important, à bien comprendre)

Un flux GBFS ne donne jamais directement l'info que vous souhaitez. Il fonctionne en deux temps, comme un sommaire de livre qui vous renvoie vers le bon chapitre :

1. Vous appelez d'abord un fichier « racine », appelé le **fichier de découverte** (`gbfs.json`). Il ne contient aucune donnée sur les vélos : il contient une **liste d'adresses** (URLs) vers les vrais fichiers de données.
2. Vous lisez cette liste, vous identifiez l'URL qui vous intéresse (par exemple celle qui donne l'état des stations), et c'est seulement là que vous allez chercher la donnée utile.

**Pourquoi faire aussi compliqué ?** Parce que l'opérateur peut changer l'adresse exacte des fichiers de données sans prévenir personne — tant que le fichier de découverte, lui, reste stable. Si ton code appelle toujours le fichier de découverte d'abord, il ne cassera jamais, même si l'opérateur change l'organisation de ses données derrière. **C'est une bonne pratique professionnelle que vous devez retenir : ne jamais coder en dur l'adresse d'une sous-ressource si un point d'entrée stable existe.**

### 1.3 Regardez la donnée dans votre navigateur, maintenant, avant de coder

1. Ouvrez votre navigateur (Chrome, Firefox, peu importe).
2. Collez cette adresse — c'est le fichier de découverte **officiel et réel** du réseau Vélo STAR, référencé sur le Point d'Accès National aux données de transport du Ministère (`transport.data.gouv.fr`) :

```
https://eu.ftp.opendatasoft.com/star/gbfs/gbfs.json
```

3. Regardez ce qui s'affiche. C'est un fichier JSON. vous devez y voir une structure qui ressemble à ceci (les valeurs exactes peuvent légèrement varier) :

```json
{
  "last_updated": 1735000000,
  "ttl": 60,
  "data": {
    "fr": {
      "feeds": [
        { "name": "system_information", "url": "https://.../system_information.json" },
        { "name": "station_information", "url": "https://.../station_information.json" },
        { "name": "station_status", "url": "https://.../station_status.json" }
      ]
    }
  }
}
```

4. **Repèrez vous-même**, dans la liste `feeds`, les deux adresses qui nous intéressent aujourd'hui :
   - celle nommée **`station_information`** → décrit les stations elles-mêmes (nom, position, capacité). Ça change rarement (une station ne déménage pas tous les jours). → **on l'utilisera en section 8, en chargement manuel.**
   - celle nommée **`station_status`** → donne l'état à l'instant présent (vélos dispo, places libres). Ça change en permanence. → **c'est celle qu'on va automatiser avec Airflow.**

5. Copiez/collez l'URL de `station_status` dans votre navigateur. Vous devez voir une structure du type :

```json
{
  "last_updated": 1735000000,
  "ttl": 60,
  "data": {
    "stations": [
      {
        "station_id": "123",
        "num_bikes_available": 4,
        "num_docks_available": 11,
        "is_installed": 1,
        "is_renting": 1,
        "is_returning": 1,
        "last_reported": 1735000000
      }
    ]
  }
}
```

> ✅ **Checkpoint** — Si vous voyez bien une liste (`data.stations`) avec un objet par station, avec un `station_id` et un nombre de vélos disponibles : vous avez compris la donnée, vous pouvez continuer. Si le format que vous observez diffère légèrement de l'exemple ci-dessus (l'opérateur peut faire évoluer ses champs), **notez les vrais noms de champs que vous voyez** : c'est cette réalité-là qu'il faudra coder, pas l'exemple du Lab. En entreprise, c'est toujours comme ça : on code contre la donnée réelle observée, jamais contre une documentation qu'on suppose à jour.

### 1.4 Pourquoi `station_id` ne suffit pas tout seul ?

Remarquez bien que le fichier `station_status` ne contient **aucun nom de station** — seulement un identifiant technique (`station_id`). Pour savoir que la station `123` s'appelle par exemple *« République »*, il faut aller chercher cette information ailleurs : dans `station_information`. C'est exactement la distinction vue en cours entre :

- une **donnée de fait** (*fact*) : ce qui se mesure et change tout le temps → ici, l'état des stations ;
- une **donnée de référence** (*dimension*) : ce qui décrit le contexte et change rarement → ici, le nom et la localisation des stations.

Vous allez traiter ces deux données différemment dans ce Lab : l'une par pipeline automatisé (**section 5**), l'autre par chargement manuel (**section 8**) — puis vous les recollerez ensemble à la fin (**section 9**), comme un vrai data analyst le ferait.

---

## 2. Installer les outils

Cette étape est importante : avant de construire un pipeline Data Lake, vous devez comprendre **l'environnement dans lequel les outils vont s'exécuter**.

Dans ce Lab, nous allons utiliser **Docker** pour exécuter MinIO, Airflow et Jupyter de manière reproductible.

### 2.1 Docker, qu'est-ce que c'est ?

**Docker est une technologie qui permet d'exécuter des applications dans des conteneurs.**

Un conteneur est un environnement isolé qui contient tout ce dont une application a besoin pour fonctionner : l'application elle-même, ses bibliothèques, ses dépendances et une partie de sa configuration.

Imaginez par exemple que vous deviez installer manuellement les trois outils du Lab :

```text
MinIO
Airflow
Jupyter
```

Sans Docker, vous pourriez devoir gérer :

- différentes versions de Python ;
- des bibliothèques Python incompatibles ;
- des versions différentes de Java ou d'autres dépendances ;
- des ports réseau ;
- des services à démarrer ;
- des configurations spécifiques à Windows, macOS ou Linux.

Deux étudiants pourraient alors obtenir :

```text
Étudiant A → ça fonctionne
Étudiant B → erreur de version Python
Étudiant C → problème de dépendance
Étudiant D → problème de configuration
```

Docker permet de réduire fortement ce problème.

L'idée devient :

```text
                 IMAGE DOCKER
                     │
                     ▼
              ┌──────────────┐
              │  Conteneur   │
              │              │
              │ Application  │
              │ Dépendances  │
              │ Configuration│
              └──────────────┘
```

Tous les étudiants utilisent alors une image construite selon la même recette.

> 🧠 **À retenir :** Docker ne sert pas à faire du Data Lake. Docker sert ici à **fournir de manière reproductible l'environnement technique dans lequel notre Data Lake va fonctionner**.

---

### 2.2 Image, conteneur, volume, réseau : les 4 notions à connaître

Avant de continuer, vous devez connaître quatre notions Docker.

#### L'image

Une **image Docker** est un modèle prêt à être utilisé pour créer un conteneur.

Par exemple :

```yaml
image: minio/minio:...
```

signifie :

> « Utilise cette image pour créer le conteneur MinIO. »

On peut comparer une image à une **machine virtuelle prête à démarrer**, mais ce n'est pas exactement une machine virtuelle : les conteneurs sont beaucoup plus légers et partagent le noyau du système hôte.

---

#### Le conteneur

Le **conteneur** est une instance en cours d'exécution d'une image.

Par exemple :

```text
Image MinIO
     │
     ▼
Conteneur MinIO
     │
     ▼
MinIO fonctionne réellement
```

Dans notre projet, nous allons avoir plusieurs conteneurs :

```text
┌─────────────────────────────────────────────┐
│              Docker                         │
│                                             │
│  ┌───────────┐  ┌───────────┐  ┌─────────┐ │
│  │  MinIO    │  │  Airflow  │  │ Jupyter │ │
│  │ conteneur │  │ conteneur │  │conteneur│ │
│  └───────────┘  └───────────┘  └─────────┘ │
│                                             │
└─────────────────────────────────────────────┘
```

---

#### Le volume

Un conteneur est conçu pour être **éphémère** : on peut le supprimer et le recréer.

Mais nos données ne doivent pas disparaître lorsque nous recréons MinIO.

Nous avons donc besoin d'un **volume Docker**.

```text
Conteneur MinIO
      │
      ▼
   /data
      │
      ▼
Volume Docker
      │
      ▼
Données persistantes
```

Dans notre projet :

```yaml
volumes:
  - minio_data:/data
```

signifie :

> « Monte le volume Docker `minio_data` dans le répertoire `/data` du conteneur MinIO. »

C'est ce qui permet au Data Lake de conserver ses fichiers.

---

#### Le réseau

Les conteneurs doivent également pouvoir communiquer entre eux.

Dans notre architecture :

```text
Airflow ───────→ MinIO
   │
   │
   └────────────→ API GBFS

Jupyter ────────→ MinIO
```

Docker Compose crée automatiquement un réseau permettant aux services de se retrouver par leur nom.

Ainsi, depuis Airflow :

```text
http://minio:9000
```

désigne le service MinIO.

Attention :

```text
localhost:9000
```

et

```text
minio:9000
```

ne signifient pas la même chose.

- `localhost` désigne la machine depuis laquelle vous faites la requête.
- `minio` désigne le conteneur/service MinIO sur le réseau Docker.

C'est une notion fondamentale pour comprendre notre architecture.

---

### 2.3 Docker Compose : pourquoi l'utiliser ?

Nous avons trois services :

```text
MinIO
Airflow
Jupyter
```

Nous pourrions lancer chaque conteneur avec des dizaines d'options Docker.

Ce serait rapidement compliqué.

**Docker Compose** permet de décrire l'ensemble de l'application dans un seul fichier :

```text
docker-compose.yaml
```

Ce fichier décrit :

- quelles images utiliser ;
- quels ports exposer ;
- quels volumes monter ;
- quelles variables utiliser ;
- quelles dépendances existent entre les services ;
- comment les services doivent démarrer.

On passe donc de :

```text
docker run ...
docker run ...
docker run ...
```

à simplement :

```bash
docker compose up -d
```

Docker Compose lit le fichier YAML et construit l'environnement.

> 🎓 **C'est exactement pourquoi nous utilisons Docker dans ce Lab :** vous ne devez pas passer la moitié du TP à installer séparément trois produits complexes. Vous allez apprendre à décrire une plateforme Data dans un fichier déclaratif reproductible.

---

### 2.4 Installer Docker

#### Windows

1. **Activer WSL2** — Ouvrez PowerShell en administrateur, tapez `wsl --install`, puis redémarrez l'ordinateur (obligatoire).
2. **Installer Docker Desktop** — Téléchargez-le depuis le site officiel Docker, installez-le en laissant « Use WSL 2 » coché.
3. **Lancer et attendre** — Ouvrez Docker Desktop et patientez jusqu'à ce que le moteur soit opérationnel.
4. **Vérifier** :

```bash
docker --version
docker compose version
```

5. **Installer Git (à faire en 2.5)** — Vérifiez ensuite :

```bash
git --version
```

#### macOS

1. **Installer Docker Desktop** — Téléchargez la version correspondant à votre Mac.
2. **Lancer Docker Desktop** et attendez que le moteur soit opérationnel.
3. **Vérifier** :

```bash
docker --version
docker compose version
```

4. Vérifiez Git :

```bash
git --version
```

#### Linux

1. **Installer Docker Engine** selon votre distribution.
2. Ajouter éventuellement votre utilisateur au groupe Docker.
3. Vérifier :

```bash
docker --version
docker compose version
```

4. Vérifier Git :

```bash
git --version
```

> ⚠️ **Rappel** — À chaque redémarrage de votre ordinateur, Docker Desktop doit être lancé et son moteur doit être opérationnel avant d'exécuter `docker compose`.

---

## 2.5 Git : pourquoi et comment l'utiliser dans ce projet ?

Git est un **système de gestion de versions**.

Dans un projet Data Engineering, Git permet notamment de :

- conserver l'historique du code ;
- savoir qui a modifié quoi ;
- revenir à une version précédente ;
- travailler à plusieurs ;
- sauvegarder le projet dans un dépôt distant ;
- préparer un futur déploiement automatisé.

Dans ce Lab, Git ne sert pas directement à faire fonctionner Airflow ou MinIO.

Il sert à **gérer le projet que vous êtes en train de construire**.

### 2.5.1 Installer git

Ne confondez pas `git` et `docker`:

```text
Git
 │
 ├── versionne le code
 ├── versionne le YAML
 ├── versionne les notebooks
 └── conserve l'historique
```

alors que :

```text
Docker
 │
 ├── exécute Airflow
 ├── exécute MinIO
 └── exécute Jupyter
```

Ils sont donc complémentaires.

```text
             GIT
              │
       code + configuration
              │
              ▼
       Docker Compose
              │
              ▼
      environnement local
              │
       ┌──────┼──────┐
       ▼      ▼      ▼
    Airflow MinIO  Jupyter
```

Téléchargez git sur `git-scm.com`, gardez les options par défaut, puis vérifiez l'installation à partir de la commande : 

```bash
git --version
```

---

### 2.5.2 Initialiser le dépôt Git

Depuis le dossier du projet :

```bash
git init
```

Cette commande crée un dépôt Git local.

Vérifiez :

```bash
git status
```

Vous devriez voir les fichiers de votre projet.

---

### 2.5.3 Pourquoi avons-nous créé `.gitignore` ?

Votre fichier `.env` contient des informations qui ne doivent pas être publiées.

Votre `.gitignore` contient donc :

```text
.env
__pycache__/
.ipynb_checkpoints/
```

Cela signifie à Git :

> « Ne prends pas ces fichiers en compte lorsque je prépare un commit. »

Vérifiez avec :

```bash
git status
```

Le fichier `.env` ne doit pas apparaître parmi les fichiers à versionner.

> ⚠️ **Erreur classique :** faire `git add .` avant d'avoir vérifié son `.gitignore`, puis publier accidentellement un fichier contenant des secrets.

---

### 2.5.4 Le premier commit

Ajoutez les fichiers du projet :

```bash
git add .
```

Puis vérifiez ce qui va être enregistré :

```bash
git status
```

Si tout est correct :

```bash
git commit -m "Initialisation du Lab Data Lake Vélo STAR"
```

Un **commit** est un point de sauvegarde dans l'historique du projet.

Vous pouvez visualiser l'historique :

```bash
git log --oneline
```

Vous devriez voir votre commit.

---

### 2.5.5 Le cycle de travail Git

Pendant le Lab, retenez ce cycle :

```text
Je modifie mon code
       │
       ▼
git status
       │
       ▼
git add
       │
       ▼
git commit
```

Par exemple :

```bash
git status
git add dags/velostar_to_minio.py
git commit -m "Ajout du pipeline Velostar vers MinIO"
```

> 🧠 **Bonne pratique :** faites des commits qui correspondent à une étape logique du projet. Évitez un commit gigantesque contenant plusieurs jours de modifications sans explication.

---

### 2.5.6 GitHub et le dépôt distant

Git peut fonctionner uniquement sur votre ordinateur.

Mais en entreprise, le dépôt est généralement également hébergé sur une plateforme distante comme GitHub, GitLab ou Azure DevOps.

Le principe est :

```text
Votre ordinateur
      │
      │ git push
      ▼
Dépôt distant
      │
      ├── historique
      ├── code
      └── collaboration
```

Dans ce Lab, vous pouvez créer un dépôt GitHub privé si l'enseignant vous le demande.

Vous pourrez ensuite envoyer votre projet avec :

```bash
git remote add origin <URL_DU_DEPOT>
git branch -M main
git push -u origin main
```

> 🔐 **Attention :** même dans un dépôt privé, ne commitez pas vos vrais secrets. Le `.env` doit rester local. Vous pouvez fournir à la place un fichier `.env.example` contenant uniquement des valeurs fictives.

---

### 2.5.7 `.env` et `.env.example`

Votre projet doit idéalement contenir :

```text
.env
.env.example
```

`.env` :

```text
MINIO_ROOT_USER=minio-root-user
MINIO_ROOT_PASSWORD=minio-root-password
MINIO_BUCKET=transport-datalake
```

`.env.example` :

```text
MINIO_ROOT_USER=change-me
MINIO_ROOT_PASSWORD=change-me
MINIO_BUCKET=transport-datalake
```

Le premier est local et ignoré par Git.

Le second peut être versionné : il sert de **modèle de configuration** pour quelqu'un qui récupère le projet.

---

### 2.5.8 Ce que vous devez savoir faire à la fin du Lab

Vous devez être capable d'expliquer :

- ce qu'est Git ;
- ce qu'est un dépôt ;
- ce qu'est un commit ;
- la différence entre `git add` et `git commit` ;
- le rôle de `.gitignore` ;
- pourquoi `.env` ne doit pas être publié ;
- la différence entre dépôt local et dépôt distant ;
- à quoi servent `git push` et `git pull`.

Vous devez également savoir exécuter au minimum :

```bash
git init
git status
git add .
git commit -m "message"
git log --oneline
```

## 3.0 Préparer les fichiers de configuration

Avant de créer `docker-compose.yaml`, vous allez préparer un petit fichier de configuration qui permet de ne pas écrire les identifiants MinIO directement dans le code.

Créez un fichier **`.env`** à la racine du projet :

```text
MINIO_ROOT_USER=minio-root-user
MINIO_ROOT_PASSWORD=minio-root-password
MINIO_BUCKET=transport-datalake
```

> 🔐 **Pourquoi faire cela ?** Dans un vrai projet, les secrets ne doivent pas être dispersés dans les fichiers Python ou YAML. Ici, nous faisons un premier pas vers cette bonne pratique. Le fichier `.env` est réservé à votre environnement local et ne doit pas être publié dans un dépôt Git public.

Créez également un fichier **`.gitignore`** :

```text
.env
__pycache__/
.ipynb_checkpoints/
```

Le fichier `.env` ne sera ainsi pas envoyé accidentellement dans Git.

---

## 3. Créer le projet

1. **Créer un dossier de travail**, par exemple `datalake-velostar`, puis ouvrez un terminal à l'intérieur.
2. **Créer deux sous-dossiers** :
   ```bash
   mkdir dags
   mkdir notebooks
   ```
   - `dags/` va accueillir le script qui automatise la collecte (section 5).
   - `notebooks/` va accueillir vos analyses Jupyter (sections 7 et 9).

3. **Créer le fichier `docker-compose.yaml`** dans ce dossier, avec exactement ce contenu :


```yaml
# docker-compose.yaml
#
# Ce fichier décrit toute notre plateforme Data Lake locale.
#
# Nous allons démarrer plusieurs services :
#   1. minio      -> stockage objet / Data Lake
#   2. minio-init -> création automatique du bucket
#   3. airflow    -> orchestration du pipeline
#   4. jupyter    -> analyse des données
#
# Docker Compose crée également un réseau interne :
#
# Airflow  ──────> MinIO
# Jupyter  ──────> MinIO
#
# Les services peuvent donc communiquer entre eux en utilisant
# leur nom de service (exemple : http://minio:9000).

services:

  # ============================================================
  # 1. MINIO
  # ============================================================
  # MinIO joue le rôle de notre stockage objet compatible S3.
  # C'est lui qui contient les fichiers de notre Data Lake.
  minio:

    # On utilise une version FIXÉE de l'image.
    # Nous évitons volontairement "latest" afin que tous les
    # étudiants utilisent la même version du logiciel.
    #
    # Image tirée de quay.io (registre officiel de l'éditeur MinIO) :
    # Docker Hub a retiré l'organisation "minio" début septembre 2026,
    # quay.io reste la source de distribution officielle et gratuite.
    image: quay.io/minio/minio:RELEASE.2025-02-28T09-55-16Z

    # Démarre le serveur MinIO.
    #
    # /data              -> emplacement des objets
    # --console-address  -> interface Web d'administration
    command: server /data --console-address ":9001"

    # Les valeurs réelles sont stockées dans le fichier .env.
    # Elles ne sont donc pas écrites directement dans le YAML.
    env_file:
      - .env

    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}

    # Correspondance entre les ports de notre ordinateur
    # et ceux du conteneur.
    #
    # localhost:9000 -> API S3 MinIO
    # localhost:9001 -> console Web MinIO
    ports:
      - "9000:9000"
      - "9001:9001"

    # Persistance des données.
    #
    # Le volume "minio_data" est monté dans /data.
    # Si le conteneur MinIO est recréé, les données du Data Lake
    # restent présentes dans le volume.
    volumes:
      - minio_data:/data

    # Docker vérifie que MinIO est prêt avant de considérer
    # le service comme opérationnel.
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      timeout: 5s
      retries: 5


  # ============================================================
  # 2. INITIALISATION DE MINIO
  # ============================================================
  # Ce petit conteneur ne fait pas partie du Data Lake lui-même.
  # Il sert uniquement à effectuer une opération d'initialisation :
  # créer automatiquement le bucket "transport-datalake".
  #
  # Cela évite de créer le bucket
  # manuellement dans l'interface MinIO.
  minio-init:

    # Même registre que minio ci-dessus, pour la même raison.
    # NB : le tag de "mc" n'est pas forcément le même que celui de
    # "minio" sur quay.io -- ce sont deux images distinctes, publiées
    # à des dates différentes.
    image: quay.io/minio/mc:RELEASE.2025-08-13T08-35-41Z

    # On attend que le service MinIO soit réellement healthy.
    depends_on:
      minio:
        condition: service_healthy

    env_file:
      - .env

    # "mc" est le client en ligne de commande de MinIO.
    # Nous :
    #   1. configurons un alias "local"
    #   2. créons le bucket s'il n'existe pas
    #   3. terminons le conteneur
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 $${MINIO_ROOT_USER} $${MINIO_ROOT_PASSWORD} &&
      mc mb --ignore-existing local/$${MINIO_BUCKET} &&
      echo 'Bucket prêt.' &&
      exit 0
      "


  # ============================================================
  # 3. AIRFLOW
  # ============================================================
  # Airflow est l'orchestrateur.
  #
  # Son rôle n'est pas de stocker les données.
  # Son rôle est de déclencher et superviser notre pipeline.
  airflow:

    # Version volontairement fixée.
    image: apache/airflow:2.9.3

    # Mode "standalone" :
    # pratique pour notre environnement pédagogique local.
    command: standalone

    environment:

      # Nous ne voulons pas charger les DAGs d'exemple.
      AIRFLOW__CORE__LOAD_EXAMPLES: "false"

      # Bibliothèques Python nécessaires à notre pipeline.
      #
      # pandas  -> manipulation des données
      # pyarrow -> écriture Parquet
      # requests -> appel de l'API GBFS
      # boto3 -> communication avec MinIO via l'API S3
      _PIP_ADDITIONAL_REQUIREMENTS: "pandas pyarrow requests boto3"

      # Paramètres permettant au DAG de communiquer avec MinIO.
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      MINIO_BUCKET: ${MINIO_BUCKET}

    # Interface Web Airflow :
    #
    # localhost:8080 -> Airflow
    ports:
      - "8080:8080"

    # Le dossier local ./dags est monté dans le conteneur.
    #
    # Cela permet à Airflow de voir les DAGs que vous écrivez
    # directement sur votre ordinateur.
    volumes:
      - ./dags:/opt/airflow/dags

    # Airflow démarre après l'initialisation de MinIO.
    depends_on:
      minio-init:
        condition: service_completed_successfully


  # ============================================================
  # 4. JUPYTER
  # ============================================================
  # Jupyter constitue notre environnement d'exploration et
  # d'analyse Python.
  jupyter:

    # Version fixée pour obtenir un environnement identique
    # entre les étudiants.
    image: quay.io/jupyter/scipy-notebook:2025-02-03

    # Démarre Jupyter Notebook.
    #
    # Pour ce Lab local uniquement, nous désactivons le token
    # afin de simplifier l'accès pédagogique.
    command: start-notebook.sh --NotebookApp.token=''

    # localhost:8888 -> Jupyter
    ports:
      - "8888:8888"

    # Le dossier local ./notebooks est accessible dans Jupyter.
    volumes:
      - ./notebooks:/home/jovyan/work

    # Jupyter doit pouvoir communiquer avec MinIO.
    depends_on:
      minio-init:
        condition: service_completed_successfully

    environment:
      MINIO_ROOT_USER: ${MINIO_ROOT_USER}
      MINIO_ROOT_PASSWORD: ${MINIO_ROOT_PASSWORD}
      MINIO_BUCKET: ${MINIO_BUCKET}


# ==============================================================
# VOLUME DOCKER
# ==============================================================
# Ce volume est persistant.
#
# Il est utilisé par MinIO pour conserver les données du Data Lake.
#
# Sans volume :
#   suppression du conteneur -> risque de perte des données
#
# Avec volume :
#   suppression/recréation du conteneur -> données conservées
volumes:
  minio_data:
```

**Ce que fait chaque service, pour rappel** :
- `minio` = le data lake lui-même (stockage façon S3, en local, gratuit).
- `airflow` = l'orchestrateur qui va déclencher la collecte automatiquement, à intervalle régulier.
- `jupyter` = ton poste d'analyse pour explorer ce que le pipeline a collecté.

---

## 4. Démarrer la pile

1. **Lancer les trois conteneurs** :
   ```bash
   docker compose up -d
   ```
2. **Vérifier que tout tourne** — Docker Desktop → onglet Containers : les trois lignes doivent être « Running ».
3. **Récupérer le mot de passe Airflow** :
   ```bash
   docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
   ```
4. Ouvrez ces trois adresses pour vérifier que tout répond :

| Interface | Adresse | Identifiants |
|---|---|---|
| Airflow | `localhost:8080` | `admin` / mot de passe ci-dessus |
| MinIO (console) | `localhost:9001` | `minio-root-user` / `minio-root-password` |
| Jupyter | `localhost:8888` | — |

5. **Créer le bucket du data lake si ce n'est pas dejà le cas** — Dans MinIO (`localhost:9001`), bouton « Create Bucket », nommez-le **`transport-datalake`**.

> 🧠 **Pourquoi ce nom de bucket ?** En entreprise, un bucket est souvent nommé d'après le **domaine métier** qu'il contient (`transport-datalake`, `rh-datalake`, `finance-datalake`...), pas d'après la techno utilisée. Ça permet à n'importe quel collègue de comprendre à quoi sert le bucket, même sans connaître le détail du pipeline.

---

## 5. Le pipeline d'ingestion Vélo STAR

### 5.1 Ce que le script doit faire, étape par étape (à lire avant le code)

Avant de regarder une seule ligne de Python, voici la logique que le script doit suivre — si vous comprenez cette liste, vous comprendrez le code instantanément :

1. Appeler le fichier de découverte `gbfs.json` (celui que vous avez regardé en section 1.3).
2. Dans la liste des flux qu'il contient, retrouver automatiquement l'URL du flux `station_status` (sans la coder en dur — cf. section 1.2).
3. Appeler cette URL pour récupérer l'état actuel de toutes les stations.
4. Transformer cette liste (qui est imbriquée, au format JSON) en un tableau simple, une ligne par station — c'est ce qu'on appelle **aplatir** (*flatten*) la donnée.
5. Ajouter une colonne technique : l'horodatage exact de la collecte (`ingested_at`). **C'est indispensable** : sans cette colonne, impossible de reconstituer un historique — tu aurais juste plein de fichiers identiques sans savoir quand chacun a été capté.
6. Convertir ce tableau en Parquet.
7. Déposer ce fichier Parquet dans MinIO, dans un chemin organisé par date puis par heure — pour qu'un futur traitement puisse facilement ne relire que la journée qui l'intéresse (c'est le même principe de partitionnement que celui vu pour la donnée météo en cours).

### 5.2 Le script — construire le pipeline étape par étape

Dans cette partie, vous allez construire le DAG **progressivement**.

Pour eviter de vous faire copier-coller un long script (**Script complet en 5.2.1**), je vous propose une explication étape par étape de l'ensemble du DAG car il vous sera demander en évaluation de récupérer plus d'information que ce qui est demandé actuellement dans ce Lab.


Créez le fichier :

```text
dags/velostar_to_minio.py
```

Vous allez construire les éléments suivants :

```text
Étape 1 → imports
Étape 2 → configuration
Étape 3 → définition du DAG
Étape 4 → appel du fichier de découverte GBFS
Étape 5 → découverte dynamique de station_status
Étape 6 → récupération des stations
Étape 7 → transformation en DataFrame
Étape 8 → ajout de l'horodatage
Étape 9 → conversion en Parquet
Étape 10 → connexion à MinIO
Étape 11 → construction du chemin Data Lake
Étape 12 → dépôt du fichier dans MinIO
Étape 13 → fermeture du DAG
```

#### Étape 1 — Importer les bibliothèques nécessaires

**Explication :**

```text
### Étape 1 — Importer les bibliothèques

Cette partie importe les bibliothèques dont notre pipeline a besoin.

- `datetime` permet d'horodater les données ;
- `io` permet de créer un fichier Parquet en mémoire ;
- `os` permet de lire les paramètres de configuration ;
- `pandas` permet de transformer les données JSON en tableau ;
- `requests` permet d'appeler l'API GBFS ;
- `boto3` permet de communiquer avec MinIO via l'API S3 ;
- Airflow fournit les décorateurs permettant de définir le DAG et les tâches.
```

**Code Python :**

```python
from datetime import datetime, timezone, timedelta
import io
import os

import pandas as pd
import requests
import boto3

from airflow.decorators import dag, task
```

> 🧠 **Question :** pourquoi avons-nous besoin de `boto3` alors que MinIO n'est pas Amazon S3 ? Parce que MinIO expose une API compatible S3. Nous pouvons donc utiliser un client S3 comme `boto3`.

---

#### Étape 2 — Définir la configuration

**Explication :**

```text
### Étape 2 — Définir la configuration

Cette partie centralise les paramètres nécessaires à la connexion au Data Lake.

Nous ne voulons pas répéter ces informations dans plusieurs endroits du programme.

Les valeurs proviennent des variables d'environnement fournies par Docker Compose.
```

**Code Python :**

```python
MINIO_ENDPOINT = "http://minio:9000"
MINIO_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]

# Le SEUL lien que nous codons en dur :
# le fichier de découverte officiel GBFS de Vélo STAR.
GBFS_DISCOVERY_URL = "https://eu.ftp.opendatasoft.com/star/gbfs/gbfs.json"
```

> 🔐 **Pourquoi le lien GBFS est-il codé en dur mais pas les credentials ?** Le lien est une information publique nécessaire pour trouver la source. Les credentials sont des informations sensibles qui doivent être externalisées.

---

#### Étape 3 — Définir le DAG Airflow

**Explication :**

```text
### Étape 3 — Définir le DAG

Nous allons maintenant déclarer notre pipeline auprès d'Airflow.

Le DAG doit être exécuté toutes les 5 minutes.

Nous ajoutons également deux tentatives supplémentaires en cas d'erreur temporaire, par exemple si l'API répond lentement.
```

**Code Python :**

```python
@dag(
    dag_id="velostar_to_minio",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["transport", "velostar", "gbfs"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
)
def velostar_to_minio():
```

**À comprendre :**

```text
schedule="*/5 * * * *"
```

signifie :

> exécuter le DAG toutes les 5 minutes.

Et :

```python
retries=2
```

signifie qu'Airflow pourra retenter la tâche si elle échoue.

---

#### Étape 4 — Lire le fichier de découverte GBFS

**Explication :**

```text
### Étape 4 — Lire le fichier de découverte

Nous commençons par appeler le fichier `gbfs.json`.

Ce fichier ne contient pas directement l'état des vélos.

Il contient les adresses des différents flux GBFS.

Notre objectif est donc de récupérer cette liste afin de trouver ensuite le flux `station_status`.
```

**Code Python :**

```python
@task
def fetch_and_store():

    discovery_response = requests.get(
        GBFS_DISCOVERY_URL,
        timeout=30
    )

    discovery_response.raise_for_status()

    discovery = discovery_response.json()

    print("Fichier de découverte récupéré.")
```

> 🧠 `raise_for_status()` est important : si le serveur répond avec une erreur HTTP, notre pipeline doit le savoir au lieu de continuer silencieusement avec une réponse invalide.

---

#### Étape 5 — Trouver dynamiquement `station_status`

**Explication :**

```text
### Étape 5 — Découvrir l'URL de station_status

Nous allons maintenant parcourir la liste des flux retournée par le fichier de découverte.

Nous cherchons le flux dont le nom est `station_status`.

Nous ne codons donc pas directement l'URL de `station_status`.

C'est une bonne pratique : si l'opérateur change l'adresse exacte du flux, le fichier de découverte peut continuer à nous indiquer la nouvelle adresse.
```

**Code Python :**

```python
    data = discovery.get("data", {})

    feeds = data.get("fr", {}).get("feeds", [])

    if not feeds:
        raise ValueError(
            "Impossible de trouver la liste des flux GBFS."
        )

    feed_names = [feed.get("name") for feed in feeds]

    print("Flux disponibles :", feed_names)

    status_feed = next(
        (
            feed for feed in feeds
            if feed.get("name") == "station_status"
        ),
        None
    )

    if status_feed is None:
        raise ValueError(
            "Le flux 'station_status' est introuvable."
        )

    status_url = status_feed["url"]

    print("URL station_status :", status_url)
```

> 🎓 **Question :** pourquoi est-il préférable de découvrir l'URL plutôt que de l'écrire directement dans le code ?

---

#### Étape 6 — Récupérer l'état des stations

**Explication :**

```text
### Étape 6 — Appeler le flux station_status

Nous avons maintenant l'URL réelle du flux.

Nous allons l'appeler pour obtenir l'état de toutes les stations au moment de l'exécution du DAG.

Cette donnée représente notre **fait** : elle change régulièrement.
```

**Code Python :**

```python
    status_response = requests.get(
        status_url,
        timeout=30
    )

    status_response.raise_for_status()

    status_json = status_response.json()

    stations = (
        status_json
        .get("data", {})
        .get("stations", [])
    )

    if not stations:
        raise ValueError(
            "Le flux station_status ne contient aucune station."
        )

    print(f"{len(stations)} stations récupérées.")
```

---

#### Étape 7 — Transformer le JSON en DataFrame

**Explication :**

```text
### Étape 7 — Aplatir la donnée

Le flux GBFS nous renvoie une structure JSON contenant une liste de stations.

Nous allons transformer cette liste en tableau pandas.

Chaque station devient une ligne.

Cette opération correspond à l'idée d'**aplatir** une structure JSON afin de pouvoir la manipuler plus facilement.
```

**Code Python :**

```python
    df = pd.DataFrame(stations)

    print("Colonnes reçues :")
    print(df.columns.tolist())

    print("Nombre de lignes :", len(df))

    print(df.head())
```

---

#### Étape 8 — Ajouter `ingested_at`

**Explication :**

```text
### Étape 8 — Ajouter l'horodatage d'ingestion

Nous ajoutons maintenant une colonne `ingested_at`.

Cette colonne indique **quand notre pipeline a récupéré la donnée**.

Elle est différente de `last_reported`.

- `last_reported` indique quand la station a rapporté son propre état ;
- `ingested_at` indique quand notre Data Lake a capturé cet état.

Cette distinction est fondamentale pour construire un historique fiable.
```

**Code Python :**

```python
    ingested_at = datetime.now(timezone.utc)

    df["ingested_at"] = ingested_at.isoformat()

    ingest_date = ingested_at.strftime("%Y-%m-%d")
    ingest_hour = ingested_at.strftime("%H")

    print("Donnée collectée à :", ingested_at.isoformat())
```

---

#### Étape 9 — Convertir en Parquet

**Explication :**

```text
### Étape 9 — Convertir les données en Parquet

Nous allons maintenant convertir notre DataFrame en fichier Parquet.

Parquet est un format colonne particulièrement adapté aux environnements Data Lake.

Nous allons effectuer la conversion directement en mémoire afin d'éviter de créer un fichier temporaire sur le disque de la machine.
```

**Code Python :**

```python
    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        index=False
    )

    buffer.seek(0)

    print("Fichier Parquet préparé en mémoire.")
```

---

#### Étape 10 — Se connecter à MinIO

**Explication :**

```text
### Étape 10 — Se connecter au Data Lake

Nous allons maintenant créer une connexion S3 avec `boto3`.

Même si nous utilisons MinIO et non Amazon S3, MinIO fournit une API compatible S3.

Depuis le conteneur Airflow, MinIO est accessible avec :

`http://minio:9000`

Nous n'utilisons pas `localhost:9000` ici car Airflow et MinIO sont deux conteneurs différents.
```

**Code Python :**

```python
    s3 = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_KEY,
        aws_secret_access_key=MINIO_SECRET,
    )

    print("Connexion au Data Lake établie.")
```

---

#### Étape 11 — Construire le chemin du Data Lake

**Explication :**

```text
### Étape 11 — Organiser le fichier dans le Data Lake

Nous allons maintenant construire le chemin du fichier.

Nous utilisons un partitionnement logique :

`ingest_date=YYYY-MM-DD/ingest_hour=HH/`

L'intérêt est de pouvoir retrouver facilement les données d'une journée ou d'une heure donnée.
```

**Code Python :**

```python
    key = (
        f"velostar/station_status/"
        f"ingest_date={ingest_date}/"
        f"ingest_hour={ingest_hour}/"
        f"{ingested_at.strftime('%Y%m%dT%H%M%S')}.parquet"
    )

    print("Chemin cible :", key)
```

Cela produira par exemple :

```text
velostar/
└── station_status/
    └── ingest_date=2026-09-13/
        └── ingest_hour=22/
            └── 20260913T220500.parquet
```

---

#### Étape 12 — Déposer le fichier dans MinIO

**Explication :**

```text
### Étape 12 — Écrire le fichier dans le Data Lake

Nous avons maintenant :

1. récupéré la donnée ;
2. transformé le JSON en tableau ;
3. ajouté l'horodatage ;
4. créé un fichier Parquet en mémoire ;
5. défini son chemin dans le Data Lake.

Il ne reste plus qu'à envoyer le fichier dans le bucket MinIO.
```

**Code Python :**

```python
    s3.put_object(
        Bucket=MINIO_BUCKET,
        Key=key,
        Body=buffer.getvalue(),
    )

    print(
        f"{len(df)} stations enregistrées → "
        f"s3://{MINIO_BUCKET}/{key}"
    )
```

---

#### Étape 13 — Fermer la définition du DAG

**Explication :**

```text
### Étape 13 — Assembler la tâche et le DAG

La fonction `fetch_and_store()` contient notre traitement.

Nous devons maintenant appeler cette tâche dans le DAG, puis appeler le DAG lui-même.

Airflow pourra alors découvrir et exécuter notre pipeline.
```

**Code Python :**

```python
    fetch_and_store()


velostar_to_minio()
```

---

### 5.2.1 Le script complet

Une fois que vous avez compris chaque étape, votre fichier `dags/velostar_to_minio.py` doit contenir l'ensemble du code suivant :

```python
from datetime import datetime, timezone, timedelta
import io
import os

import pandas as pd
import requests
import boto3

from airflow.decorators import dag, task


# ============================================================
# Configuration
# ============================================================

MINIO_ENDPOINT = "http://minio:9000"
MINIO_KEY = os.environ["MINIO_ROOT_USER"]
MINIO_SECRET = os.environ["MINIO_ROOT_PASSWORD"]
MINIO_BUCKET = os.environ["MINIO_BUCKET"]

GBFS_DISCOVERY_URL = (
    "https://eu.ftp.opendatasoft.com/star/gbfs/gbfs.json"
)


# ============================================================
# DAG
# ============================================================

@dag(
    dag_id="velostar_to_minio",
    schedule="*/5 * * * *",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["transport", "velostar", "gbfs"],
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=1),
    },
)
def velostar_to_minio():

    @task
    def fetch_and_store():

        # ----------------------------------------------------
        # Étape 1 : fichier de découverte
        # ----------------------------------------------------
        discovery_response = requests.get(
            GBFS_DISCOVERY_URL,
            timeout=30
        )
        discovery_response.raise_for_status()

        discovery = discovery_response.json()

        # ----------------------------------------------------
        # Étape 2 : découverte dynamique de station_status
        # ----------------------------------------------------
        data = discovery.get("data", {})
        feeds = data.get("fr", {}).get("feeds", [])

        if not feeds:
            raise ValueError(
                "Impossible de trouver la liste des flux GBFS."
            )

        status_feed = next(
            (
                feed for feed in feeds
                if feed.get("name") == "station_status"
            ),
            None
        )

        if status_feed is None:
            raise ValueError(
                "Le flux 'station_status' est introuvable."
            )

        status_url = status_feed["url"]

        # ----------------------------------------------------
        # Étape 3 : récupération des stations
        # ----------------------------------------------------
        status_response = requests.get(
            status_url,
            timeout=30
        )
        status_response.raise_for_status()

        status_json = status_response.json()

        stations = (
            status_json
            .get("data", {})
            .get("stations", [])
        )

        if not stations:
            raise ValueError(
                "Aucune station trouvée dans station_status."
            )

        # ----------------------------------------------------
        # Étape 4 : transformation en DataFrame
        # ----------------------------------------------------
        df = pd.DataFrame(stations)

        # ----------------------------------------------------
        # Étape 5 : horodatage de l'ingestion
        # ----------------------------------------------------
        ingested_at = datetime.now(timezone.utc)

        df["ingested_at"] = ingested_at.isoformat()

        ingest_date = ingested_at.strftime("%Y-%m-%d")
        ingest_hour = ingested_at.strftime("%H")

        # ----------------------------------------------------
        # Étape 6 : conversion en Parquet
        # ----------------------------------------------------
        buffer = io.BytesIO()

        df.to_parquet(
            buffer,
            index=False
        )

        buffer.seek(0)

        # ----------------------------------------------------
        # Étape 7 : connexion à MinIO
        # ----------------------------------------------------
        s3 = boto3.client(
            "s3",
            endpoint_url=MINIO_ENDPOINT,
            aws_access_key_id=MINIO_KEY,
            aws_secret_access_key=MINIO_SECRET,
        )

        # ----------------------------------------------------
        # Étape 8 : construction du chemin
        # ----------------------------------------------------
        key = (
            f"velostar/station_status/"
            f"ingest_date={ingest_date}/"
            f"ingest_hour={ingest_hour}/"
            f"{ingested_at.strftime('%Y%m%dT%H%M%S')}.parquet"
        )

        # ----------------------------------------------------
        # Étape 9 : écriture dans le Data Lake
        # ----------------------------------------------------
        s3.put_object(
            Bucket=MINIO_BUCKET,
            Key=key,
            Body=buffer.getvalue(),
        )

        print(
            f"{len(df)} stations enregistrées → "
            f"s3://{MINIO_BUCKET}/{key}"
        )

    fetch_and_store()


velostar_to_minio()
```


### 5.3 Vérifiez que vous comprenez chaque ligne (ne passez pas à la suite avant)

Posez-vous ces questions et répondez-y avec vos propres mots — si vous bloquez sur une seule, relisez la section correspondante ci-dessus :

- Pourquoi appelle-t-on d'abord `GBFS_DISCOVERY_URL` plutôt que d'écrire directement l'URL de `station_status` ? *(→ section 1.2)*
- À quoi sert la colonne `ingested_at`, et pourquoi ne suffit-il pas d'utiliser le champ `last_reported` déjà fourni par l'API ? *(→ section 5.1, point 5)*
- Pourquoi range-t-on le fichier dans un chemin du type `ingest_date=.../ingest_hour=.../fichier.parquet` plutôt que tout au même endroit ? *(indice : imagine que tu doives relire, dans six mois, uniquement les données du 14 mars entre 8h et 9h — comment ferais-tu si tout était mélangé dans un seul dossier ?)*
- `schedule="*/5 * * * *"` veut dire quoi ? *(c'est une expression **cron** : elle se lit « à chaque fois que le nombre de minutes est un multiple de 5 »)*

---
**NB. Rendez les reponses à ces questions + le schema de l'architecture demandé plus haut dans un word/Pdf/Slide et déposez-le sur CESAR avant la fin de la journée.**

## 6. Déclencher et vérifier

1. **Attendre la détection** — Airflow scanne le dossier `dags/` toutes les ~30 secondes. Patientez une minute, si rien ne se passe, déconnectez-vous puis reconnectez-vous à airflow.
2. **Activer le DAG** — Sur `localhost:8080`, trouvez `velostar_to_minio`, basculez l'interrupteur sur ON.
3. **Déclencher une première exécution manuellement** — Cliquez sur le DAG, puis sur ▶ (Trigger DAG).
4. **Vérifier le résultat** — Cercle vert = succès. Regardez les logs de la tâche `fetch_and_store` : vous devez voir s'afficher une ligne du type `XX stations enregistrées → s3://transport-datalake/velostar/...`.
5. Allez dans MinIO (`localhost:9001` → bucket `transport-datalake`) : vous devez voir apparaître l'arborescence `velostar/station_status/ingest_date=.../ingest_hour=.../....parquet`.
6. **Laissez tourner le pipeline pendant le reste du Lab** (il se déclenchera tout seul toutes les 5 minutes) : c'est ce qui va vous permettre, en section 7, d'observer un **vrai historique qui s'accumule**, avec des valeurs différentes à chaque exécution — la preuve que votre data lake fait exactement ce pour quoi on vous a embauché.

---

## 7. Explorer l'historique dans Jupyter

Sur `localhost:8888`, créez un notebook Python 3, puis exécutez dans l'ordre.

### 7.1 Lister tout ce qui a déjà été collecté

```python
!pip install boto3 --quiet
```

```python
import boto3
import pandas as pd
import io

s3 = boto3.client(
    "s3",
    endpoint_url="http://minio:9000",
    aws_access_key_id="minio-root-user",
    aws_secret_access_key="minio-root-password",
)

paginator = s3.get_paginator("list_objects_v2")
fichiers = []

for page in paginator.paginate(
    Bucket="transport-datalake",
    Prefix="velostar/station_status/",
):
    fichiers.extend(obj["Key"] for obj in page.get("Contents", []))

print(f"{len(fichiers)} fichier(s) collecté(s) jusqu'à présent :")
for f in fichiers:
    print(" -", f)
```

> ✅ **Checkpoint** — Si le pipeline tourne depuis un moment, vous devez voir **plusieurs fichiers**, avec des horodatages différents dans leur nom. Si vous n'en voyez qu'un seul, patientez encore 5 minutes (ou redéclenchez manuellement le DAG depuis Airflow) puis relancez cette cellule.

### 7.2 Charger et empiler tous les fichiers en un seul historique

```python
# On lit chaque fichier Parquet et on les empile en un seul grand tableau :
# c'est cette étape qui "reconstruit" l'historique que l'API seule ne garde jamais.
dataframes = []
for cle in fichiers:
    obj = s3.get_object(Bucket="transport-datalake", Key=cle)
    dataframes.append(pd.read_parquet(io.BytesIO(obj["Body"].read())))

historique = pd.concat(dataframes, ignore_index=True)
print(f"{len(historique)} lignes au total, pour {historique['station_id'].nunique()} stations distinctes")
historique.head()
```

### 7.3 Une première observation simple

```python
# Évolution du nombre de vélos disponibles sur UNE station, au fil des collectes
un_id_station = historique["station_id"].iloc[0]
(
    historique[historique["station_id"] == un_id_station]
    [["ingested_at", "num_bikes_available", "num_docks_available"]]
    .sort_values("ingested_at")
)
```

> 🧠 **Ce que vous venez de prouver** : avant ce Lab, cette information n'existait nulle part — l'API ne donne que l'instant présent. Le tableau que vous obtenez ici (l'évolution dans le temps) **n'existe que parce que votre data lake l'a capturée**. C'est exactement la valeur ajoutée d'un data lake face à une simple API : transformer une donnée éphémère en historique exploitable.

---

## 8. Chargement manuel du référentiel des stations (donnée de dimension)

### 8.1 Pourquoi ce chargement est manuel, et pas automatisé comme le reste

Le nom, la localisation et la capacité des stations (le fichier `station_information` repéré en section 1.3) changent très rarement — seulement quand l'opérateur ouvre, ferme ou déplace une station. Automatiser sa collecte toutes les 5 minutes comme pour `station_status` serait donc inutile et gaspillerait des ressources. En entreprise, ce type de référentiel est très souvent chargé **manuellement ou selon une fréquence beaucoup plus faible** (une fois par semaine, ou à la demande) — c'est un choix d'architecture assumé, pas un oubli.

### 8.2 Récupérer le fichier

1. Dans votre navigateur, retournez sur le fichier de découverte utilisé en section 1.3 :
   ```
   https://eu.ftp.opendatasoft.com/star/gbfs/gbfs.json
   ```
2. Repèrez cette fois l'URL du flux nommé **`station_information`**.
3. Ouvrez cette URL dans votre navigateur : vous devez voir un JSON avec, pour chaque station, son `station_id`, son `name`, ses coordonnées `lat`/`lon`, et sa `capacity`.
4. Enregistrez ce résultat sur votre poste sous le nom `station_information.json` (dans la plupart des navigateurs : clic droit → Enregistrer sous, ou `Ctrl/Cmd+S`).

### 8.3 Déposer ce fichier manuellement dans le data lake

1. Ouvrez MinIO (`localhost:9001`), entrez dans le bucket `transport-datalake`.
2. Créez un chemin logique dédié au chargement manuel : `reference/station_information/`.
3. Cliquez sur **Upload → Upload File**, sélectionnez `station_information.json`, déposez-le dans `reference/station_information/`.

> 💡 **Bonne pratique** : on distingue bien, par le chemin, ce qui vient du pipeline automatisé (`velostar/...`) de ce qui a été déposé manuellement (`reference/...`). N'importe quel collègue qui explore le bucket comprend immédiatement, rien qu'en lisant les chemins, quelles données sont rafraîchies automatiquement et lesquelles ne le sont pas.

### 8.4 Relire ce fichier dans Jupyter

```python
obj = s3.get_object(Bucket="transport-datalake", Key="reference/station_information/station_information.json")
import json
info_json = json.loads(obj["Body"].read())
stations_info = pd.DataFrame(info_json["data"]["stations"])
stations_info[["station_id", "name", "lat", "lon", "capacity"]].head()
```

> ✅ **Checkpoint** — Vous devez maintenant avoir deux tableaux distincts en mémoire dans votre notebook : `historique` (le fait, automatisé, plein de lignes qui se répètent dans le temps) et `stations_info` (la dimension, chargée une fois à la main, une seule ligne par station).

---

## 9. Croiser les données et répondre à une vraie question métier

C'est l'étape qui donne du sens à tout le reste : on va enfin répondre à la question posée par la direction Mobilités en section 0.

### 9.1 Fusionner fait et dimension

```python
# On enrichit chaque ligne d'historique avec le nom de la station correspondante.
# C'est une JOINTURE classique, comme en SQL, mais faite ici en pandas.
historique_enrichi = historique.merge(
    stations_info[["station_id", "name", "capacity"]],
    on="station_id",
    how="left",
)
historique_enrichi[["ingested_at", "station_id", "name", "num_bikes_available", "num_docks_available"]].head()
```

### 9.2 Répondre à la vraie question métier : quelles stations sont le plus souvent vides ?

```python
# Une station est considérée "vide" quand num_bikes_available == 0
historique_enrichi["est_vide"] = historique_enrichi["num_bikes_available"] == 0

taux_saturation = (
    historique_enrichi
    .groupby("name")["est_vide"]
    .mean()  # proportion des relevés où la station était vide
    .sort_values(ascending=False)
    .rename("proportion_de_temps_vide")
)
taux_saturation.head(10)
```

> 🧠 **Ce que vous venez de produire** : un tableau directement lisible et actionnable par la direction Mobilités — "voici, sur la période observée, les stations les plus souvent à sec". C'est une réponse **beaucoup plus riche que ce que l'API seule pouvait fournir à l'instant T**, et elle n'a été possible que parce que : (1) vous avez construit un historique via le data lake, (2) vous l'avez enrichi avec un référentiel chargé manuellement. Ce sont très exactement ces deux ingrédients — capture automatisée du fait + référentiel de dimension — qui structurent la quasi-totalité des projets data en entreprise, transport ou non.

---
## 9.5. Evaluation individuelle notée (10 points)
Créez un second DAG qui récupère les données suivantes :

```
system_information ==> Informations sur le système
system_pricing_plans ==> Le plan tarifaire est potentiels abonnements
system_alerts ==> Comment sont remointées les alertes ?
```
---

## 10. Dépannage — problèmes réels rencontrés

**« unable to get image » / le moteur Docker ne répond pas**
Docker Desktop n'est pas ouvert ou pas encore prêt. Ouvrez l'application, attendez « Engine running », relancez la commande.

**Une image Docker est introuvable (« manifest unknown », « not found »)**
Privilégiez toujours les images officielles (comme `minio/minio` utilisée ici) plutôt que des versions reconditionnées par un tiers.

**Le DAG passe en échec (case rouge) avec une erreur réseau ou timeout**
L'API GBFS peut être temporairement lente ou indisponible (c'est un vrai service en production, pas un service de démonstration figé). Redéclenche manuellement l'exécution. Si le problème persiste plus de 10 minutes, vérifiez que `https://eu.ftp.opendatasoft.com/star/gbfs/gbfs.json` répond bien dans votre navigateur — si ce n'est pas le cas, c'est le service source qui est temporairement indisponible, pas votre pipeline qui est en cause.

**La liste `feeds` ne contient pas de flux nommé exactement `station_status` ou `station_information`**
Les opérateurs font parfois évoluer très légèrement le nommage. Affichez la liste complète des noms disponibles avec `[f["name"] for f in feeds]` et adaptez le nom recherché dans le code en conséquence — c'est exactement la situation décrite en section 1.3 : on code contre la donnée réellement observée.

**Le fichier Parquet dans MinIO est vide ou l'exécution réussit mais 0 station n'est enregistrée**
Vérifiez, dans les logs de la tâche Airflow, ce qu'a réellement renvoyé `status_response.json()`. Affichez `stations[:2]` avant de construire le DataFrame pour voir si la structure correspond bien à celle attendue en section 1.3.

**Identifiants Airflow inconnus / connexion refusée**
```bash
docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt
```

**Je veux tout remettre à zéro proprement**
```bash
docker compose down -v
```

---

## 11. Pour aller plus loin

- **🚏 Élargir à d'autres modes de transport** — Le même GBFS existe pour d'autres opérateurs de vélos/trottinettes en libre-service dans d'autres villes : il suffit de changer l'URL du fichier de découverte. Le réseau STAR publie aussi des flux temps réel pour ses parcs-relais et l'état des lignes de métro (visibles sur `data.explore.star.fr`) — même logique d'ingestion à appliquer.
- **📈 Visualiser** — Trace, dans Jupyter, l'évolution du nombre de vélos disponibles sur une station précise au fil de la journée avec `matplotlib`, une fois plusieurs heures de données accumulées.
- **🗺️ Cartographier** — En croisant `lat`/`lon` (venant de `station_information`) avec le taux de saturation calculé en section 9.2, tu peux produire une carte des stations à risque.
- **🗄️ SQL sur le lac** — Un moteur comme Trino permettrait d'interroger directement, en SQL, l'ensemble des fichiers Parquet accumulés, sans avoir à tous les charger en mémoire comme on l'a fait en section 7.2.
- **⏱️ Automatiser aussi le référentiel** — Rien n'empêche, plus tard, de créer un second DAG qui rafraîchit `station_information` une fois par jour (au lieu de le charger manuellement une seule fois) : ce serait la suite logique de ce TP.

---

## 12. Lexique de secours

| Terme | Explication simple |
|---|---|
| GBFS | Standard international de publication des données de vélos/trottinettes en libre-service |
| Fichier de découverte (discovery file) | Fichier « sommaire » qui indique où trouver les vrais fichiers de données |
| Aplatir (flatten) | Transformer une structure JSON imbriquée en tableau simple, une ligne par élément |
| Donnée de fait | Donnée qui se mesure et change souvent (ex. nombre de vélos disponibles) |
| Donnée de dimension / référentiel | Donnée descriptive qui change rarement (ex. nom d'une station) |
| Partitionnement | Organisation des fichiers par dossiers (ex. par date) pour accélérer et cibler la lecture |
| Jointure (merge/join) | Opération qui combine deux tableaux à partir d'une colonne commune (ici, `station_id`) |
| Bucket | Un casier de rangement dans un système de stockage façon S3 |
| Parquet | Format de fichier compact et rapide à lire pour des données tabulaires |
| DAG | Une suite programmée de tâches automatiques, dans Airflow |
| cron | Notation standard (`*/5 * * * *`) pour exprimer une fréquence de déclenchement |


## 13. Validation finale du Lab

Avant de déposer votre compte-rendu, vérifiez que vous êtes capable de démontrer chacun des points suivants.

### Infrastructure

- [ ] Docker fonctionne sur votre machine.
- [ ] Les services MinIO, Airflow et Jupyter sont démarrés.
- [ ] Le bucket `transport-datalake` existe.
- [ ] Airflow est accessible sur `localhost:8080`.
- [ ] MinIO est accessible sur `localhost:9001`.
- [ ] Jupyter est accessible sur `localhost:8888`.

### Pipeline

- [ ] Le DAG `velostar_to_minio` est visible dans Airflow.
- [ ] Le DAG peut être déclenché manuellement.
- [ ] Le DAG s'exécute avec succès.
- [ ] Les données sont récupérées depuis le fichier de découverte GBFS.
- [ ] L'URL `station_status` est découverte dynamiquement.
- [ ] Un fichier Parquet est créé à chaque exécution.
- [ ] Les fichiers sont stockés dans MinIO.
- [ ] Le chemin contient la date et l'heure d'ingestion.

### Analyse

- [ ] Plusieurs fichiers Parquet sont visibles après plusieurs exécutions.
- [ ] Jupyter peut lire ces fichiers depuis MinIO.
- [ ] L'historique contient plusieurs instants d'observation.
- [ ] Le référentiel `station_information` a été chargé.
- [ ] La jointure sur `station_id` fonctionne.
- [ ] Le taux de stations vides est calculé.
- [ ] Vous pouvez expliquer la différence entre `last_reported` et `ingested_at`.

### Compte-rendu (10 points)

Votre rendu doit contenir :

1. le schéma de l'architecture ;
2. les réponses aux questions de compréhension ;
3. une capture de l'interface Airflow montrant le DAG en succès ;
4. une capture de MinIO montrant les fichiers Parquet ;
5. une capture du notebook montrant l'historique ;
6. le résultat de l'analyse des stations les plus souvent vides ;
7. une courte conclusion expliquant la valeur ajoutée du Data Lake ;


---

## 14. Préparation du Lab 2 — Approche S3 sur Lakehouse Databricks

Ce premier TP reste volontairement **local**. Vous avez appris à construire et faire fonctionner le pipeline.

Dans le TP 2, vous reprendrez exactement ce projet mais cette fois avec une stack purement cloud.

---

*Support pédagogique — Module Data Lake, M2 Data, Sup de Vinci. Cas d'usage construit à partir de données réelles et ouvertes du réseau Vélo STAR (Rennes Métropole / Keolis), référencées sur transport.data.gouv.fr, sous licence ODbL.*

**Intervenant : [Franck ORAGA](https://www.linkedin.com/in/franck-oraga-b2a200196/), Consultant senior data plateform, CEO de EXOR DATA.**

**Date : lundi 21 septembre 2026**