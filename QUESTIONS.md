# Questions

Avant de répondre aux questions du projet Data, il est conseillé de lire le fichier `description-des-bases-de-donnees-annuelles.pdf` qui décrit les différentes tables et leur contenu.

## Question 1 - Caractéristiques de l'accident

Lire la donnée `caract-2024.csv` et afficher les 5 premières lignes. Quelle est la taille de la donnée ? Combien de colonnes ? Combien de lignes ?

En vous basant sur le fichier `description-des-bases-de-donnees-annuelles.pdf`, transformer la donnée `caract-2024.csv` conformément à la description des différents champs.

Par exemple, le champ lum est décrit de la manière suivante:

```
lum
Lumière : conditions d’éclairage dans lesquelles l'accident s'est produit :
1 – Plein jour
2 – Crépuscule ou aube
3 – Nuit sans éclairage public
4 – Nuit avec éclairage public non allumé
5 – Nuit avec éclairage public allumé
```

Ainsi, la valeur 1 pour le champ lum devrait être transformée en "Plein jour", la valeur 2 en "Crépuscule ou aube", etc. 

Appliquez l'ensemble des transformations nécessaires à la donnée `caract-2024.csv` pour la transformer conformément à la description des différents champs.

## Question 2 - Caractéristiques de l'accident

Analysez les données transformées `caract-2024.csv` et répondez aux questions suivantes:

1. Combien d'accidents ont eu lieu en 2024 ?
2. Combien d'accidents ont eu lieu en 2024 en agglomération ?
3. Affichez le nombre d'accidents par critère de luminosité.

## Question 3 - Lieux de l'accident

Lire la donnée `lieux-2024.csv` et afficher les 5 premières lignes. Quelle est la taille de la donnée ? Combien de colonnes ? Combien de lignes ?

En vous basant sur le fichier `description-des-bases-de-donnees-annuelles.pdf`, transformer la donnée `lieux-2024.csv` conformément à la description des différents champs.

Questions de preprocessing:
1. Comment gérer les valeurs manquantes dans les champs numériques (par exemple `pr`, `pr1`, `v1`) ?
2. Quelles transformations sont nécessaires pour les champs catégoriels (catr, circ, vosp, prof, etc.) ?
3. Comment nettoyer et valider les données avant transformation ?

Analysez les données transformées `lieux-2024.csv` et répondez aux questions suivantes:
1. Combien d'accidents ont eu lieu sur autoroute ?
2. Affichez la répartition des accidents par type de route.
3. Quelle est la vitesse maximale autorisée moyenne sur les lieux d'accidents ?

## Question 4 - Usagers de l'accident

Lire la donnée `usagers-2024.csv` et afficher les 5 premières lignes. Quelle est la taille de la donnée ? Combien de colonnes ? Combien de lignes ?

En vous basant sur le fichier `description-des-bases-de-donnees-annuelles.pdf`, transformer la donnée `usagers-2024.csv` conformément à la description des différents champs.

Questions de preprocessing:
1. Comment calculer l'âge à partir de l'année de naissance (`an_nais`) ?
2. Comment gérer les valeurs manquantes dans les champs d'équipement de sécurité (secu1, secu2, secu3) ?
3. Quelles transformations sont nécessaires pour les champs catégoriels (catu, grav, sexe, trajet, etc.) ?

Analysez les données transformées `usagers-2024.csv` et répondez aux questions suivantes:
1. Quelle est la répartition des accidents par gravité ?
2. Combien d'usagers étaient des piétons ?
3. Affichez la répartition par catégorie d'usager (conducteur, passager, piéton).

## Question 5 - Véhicules de l'accident

Lire la donnée `vehicules-2024.csv` et afficher les 5 premières lignes. Quelle est la taille de la donnée ? Combien de colonnes ? Combien de lignes ?

En vous basant sur le fichier `description-des-bases-de-donnees-annuelles.pdf`, transformer la donnée `vehicules-2024.csv` conformément à la description des différents champs.

Questions de preprocessing:
1. Comment gérer les valeurs invalides dans le champ `senc` (sens de circulation) ?
2. Quelles transformations sont nécessaires pour les champs catégoriels (catv, obs, obsm, choc, manv, etc.) ?
3. Comment nettoyer le champ `occutc` (nombre d'occupants) ?

Analysez les données transformées `vehicules-2024.csv` et répondez aux questions suivantes:
1. Quelle est la répartition des accidents par catégorie de véhicule ?
2. Combien d'accidents impliquent des deux-roues motorisés ?
3. Affichez la répartition par type de motorisation.

## Question 6 - Jointure des données

Joindre les données transformées (silver) pour créer un dataset unifié (gold).

1. Quelles sont les clés de jointure entre les différentes tables ?
2. Quel type de jointure utiliser (inner, left, right, outer) et pourquoi ?
3. Combien d'enregistrements contient le dataset gold après jointure ?
4. Y a-t-il des incohérences ou des valeurs manquantes après la jointure ?

## Question 7 - Analyse des données (format Gold)

Analysez le dataset gold (après jointure) et répondez aux questions suivantes:

1. **Analyse temporelle:**
   - À quelles heures de la journée y a-t-il le plus d'accidents ?
   - Y a-t-il des jours de la semaine plus dangereux que d'autres ?
   - Quelle est l'évolution mensuelle des accidents en 2024 ?

2. **Analyse géographique:**
   - Quels sont les 10 départements avec le plus d'accidents ?
   - Quelle est la répartition des accidents entre agglomération et hors agglomération ?

3. **Analyse des usagers:**
   - Quelle est la gravité moyenne des accidents selon la catégorie d'usager ?
   - Y a-t-il une corrélation entre l'âge et la gravité de l'accident ?
   - Combien d'usagers portaient un équipement de sécurité au moment de l'accident ?

4. **Analyse des véhicules:**
   - Quelles catégories de véhicules sont impliquées dans les accidents les plus graves ?
   - Y a-t-il une relation entre le type de motorisation et la gravité de l'accident ?

5. **Analyse croisée:**
   - Quelle est la combinaison la plus fréquente entre type de route et catégorie de véhicule impliquée ?
   - Y a-t-il une relation entre les conditions atmosphériques et la gravité des accidents ?

## Question 8 - Afficher une carte

Générer une carte interactive affichant les accidents routiers avec le module `folium`.

1. Filtrer les accidents pour une zone géographique spécifique (par exemple, Paris ou un département).
2. Afficher les accidents sur une carte en utilisant les coordonnées latitude/longitude.
3. Colorer les points selon le critère de gravité de l'accident.
4. Ajouter des informations au survol (tooltip) avec les détails de l'accident.

**Exemple:** Afficher tous les accidents survenus à Paris en 2024, colorés par gravité.

## Question 9 - Pour aller plus loin

Utilisant `dagster`, créez un pipeline qui permet de traiter les données et de les stocker avec une approche médaillon:
- **Raw:** Données brutes téléchargées depuis data.gouv.fr
- **Silver:** Données transformées et nettoyées (une table par source)
- **Gold:** Données jointes et enrichies (dataset unifié)
- **Analytics:** Analyses et visualisations (cartes, statistiques, etc.)

Le pipeline doit inclure:
1. Téléchargement des données (raw)
2. Preprocessing de chaque source (silver)
3. Jointure des données (gold)
4. Génération des analyses et visualisations (analytics)