# Master MIMO - Projet data

## Installation

Pour installer le projet, nous allons utiliser uv (0.8.15):

```bash
uv sync --all-groups
```

# Lancer le script main.py
```bash
uv run python -m data_project.main
```

# Lancer le workflow dagster

```bash
export DAGSTER_HOME=</path/to/your/project/directory>
uv run dg launch --job data_project_workflow
```

Un dossier `data` sera créé à la racine du projet, contenant :
- `raw` : données brutes provenant de data.gouv.fr
- `silver` : données traitées
- `gold` : données jointes
- `analytics` : la carte des accidents parisiens

Ou lancer le workflow dagster avec le web interface :

```bash
export DAGSTER_HOME=</path/to/your/project/directory>
uv run dg dev --verbose
```