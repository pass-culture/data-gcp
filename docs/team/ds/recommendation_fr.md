# Le moteur de recommandation Pass Culture

Notre moteur de recommandation vise à mettre en avant, parmi notre riche base de **2 millions de biens et événements culturels**, des éléments pertinents et personnalisés pour nos **xM utilisateurs**.

Nous utilisons la technique de **filtrage collaboratif** pour recommander des offres culturelles aux utilisateurs en fonction de leurs interactions avec l'application.

> **Le filtrage collaboratif** est une technique utilisée par les systèmes de recommandation pour prédire les intérêts d'un utilisateur en collectant les préférences de nombreux utilisateurs (collaboration) et en extrapolant ces données pour faire des prédictions sur les intérêts d'un utilisateur donné.

### Fonctionnalités principales :
- **Recommandations personnalisées** : des recommandations adaptées à un utilisateur donné.
- **Recommandations d'offres similaires** : des suggestions d'offres similaires à une offre donnée.

---

## Les algorithmes

### L'architecture du pipeline

Nous utilisons un modèle de **Récupération-Classement**, qui récupère une liste d'éléments à partir d'un large corpus et les classe par ordre de pertinence par rapport à une requête donnée, comme illustré ci-dessous.

![Modèle de récupération-classement](assets/retrieval-ranking-architecture.png)

En effet, le modèle est composé de trois parties :

* Un **modèle de récupération** : il récupère une liste d'éléments à partir d'un très large corpus. Pensez à des millions d'éléments réduits à quelques centaines.
    > Le modèle n'a pas besoin d'être très précis, mais il doit être très rapide et évolutif.
* Un **modèle de classement** : il classe les éléments récupérés par ordre de pertinence par rapport à une requête donnée. Dans notre cadre, nous l'utilisons également pour filtrer les éléments non pertinents pour l'utilisateur, afin de conserver moins d'une centaine de résultats.
    > Le modèle doit être très précis, mais il peut être lent et moins évolutif.
* Un **modèle post-classement** : il sélectionne des offres provenant de différentes catégories et/ou sous-catégories. L'objectif est de garantir une distribution plus équitable et diversifiée des offres par catégories (différents artistes, lieux, types d'offres, etc.). Cette combinaison peut être réalisée par catégories, sous-catégories, GTL (catégorisation des livres) ou par groupes de clusters préalablement entraînés sur la sémantique des offres. Le cas par défaut est celui des clusters (500 clusters sur 4 millions d'éléments).

---

### Le modèle de récupération

Nous utilisons un **modèle à deux tours** (Two-Tower Model), qui est la norme dans l'industrie. Voir l'article original de [YouTube Recommendation](https://static.googleusercontent.com/media/research.google.com/fr//pubs/archive/45530.pdf).

#### Le modèle à deux tours

##### Vue d'ensemble

L'approche Two-Tower représente une stratégie hybride de recommandation, combinant les forces du filtrage collaboratif traditionnel et des systèmes de recommandation basés sur le contenu. Elle encode à la fois les caractéristiques des utilisateurs et des éléments dans des vecteurs denses appelés **tours**.

En essence, elle apprend les préférences des utilisateurs non seulement à partir de leurs interactions avec les éléments, mais aussi à partir des propriétés inhérentes des utilisateurs et des éléments.

##### Encodage des caractéristiques (couches)

* Les caractéristiques des utilisateurs et des éléments sont converties en représentations mathématiques à l'aide de **"couches"**.
* Différents types de couches dictent comment les caractéristiques sont "traduites".
* La taille des embeddings (`embedding_size`) détermine la dimension du vecteur représentant une caractéristique.
* **Des couches empilées forment les tours utilisateur et élément.**

##### Configuration par défaut des caractéristiques et des couches

Devrions-nous ajouter notre configuration ici ?

---

### Entraînement et validation

Le modèle est entraîné en utilisant une combinaison de :
- **Données d'interaction utilisateur**
- **Informations sur le profil utilisateur**
- **Métadonnées des éléments**

#### Fréquence d'entraînement :
- Le modèle est réentraîné **chaque semaine** pour rester à jour avec le comportement des utilisateurs et les mises à jour du catalogue.

#### Fenêtre de données :
- **Données de réservation** des **6 derniers mois** pour capturer une vue d'ensemble des intérêts des utilisateurs.
- Accent mis sur les **3 derniers mois** de **données de clics** pour accorder plus de poids aux tendances actuelles des utilisateurs.

#### Processus d'entraînement :
- **Métrique Top-k** : Évalue la capacité du modèle à classer les éléments consommés parmi les premiers résultats pour un utilisateur donné.
- **Objectif d'optimisation** : Maximiser le classement des éléments consommés.

> **Distinction clé** :
> Le modèle utilise les **données de clics** pour l'entraînement, mais les **données de réservation** pour la validation.

---

### Le modèle de classement

Dans le contexte de la recommandation, un **modèle de classification** est utilisé pour prédire la probabilité d'actions spécifiques des utilisateurs, telles que cliquer sur ou réserver un élément.

#### Modèle de classification

- Exploite des **caractéristiques utilisateur et élément**, telles que :
  - Les préférences des utilisateurs
  - Les métadonnées des éléments
  - Les informations contextuelles (par exemple, l'heure de la journée, la distance à l'offre)
- Traite ces caractéristiques pour distinguer les éléments pertinents pour l'utilisateur de ceux qui ne le sont pas.
- La sortie est utilisée comme un **signal de classement**, où les éléments avec des probabilités plus élevées sont classés plus haut.

---

#### Modèle de prédiction des clics et des réservations

Nous utilisons ce modèle pour prédire :
- La probabilité d'un **clic**.
- La probabilité d'une **réservation**.

Le score final est calculé comme suit :

$$ s = P(clic) + P(réservation) $$

> Ce modèle plus simple a montré de meilleures performances et permet une itération plus facile.

---

### Le modèle post-classement

Le **modèle post-classement** garantit la diversité et l'équité en sélectionnant des offres provenant de différentes catégories, sous-catégories ou clusters. Cela permet une distribution plus équitable des offres, telles que :
- Différents artistes
- Lieux
- Types d'offres

---

### Métriques

#### Métriques hors ligne :
- **Précision Top-k** : Mesure la capacité du modèle à classer les éléments pertinents parmi les premiers résultats.
- **Métriques de diversité** : Garantit une sélection variée d'offres dans les recommandations.
- **Métriques d'engagement** : Suit les interactions des utilisateurs avec les offres recommandées.

---

### Résumé

Le moteur de recommandation Pass Culture combine **filtrage collaboratif**, **modèles de récupération-classement** et **diversification post-classement** pour offrir des recommandations personnalisées et diversifiées aux utilisateurs. En réentraînant et en validant continuellement le modèle, nous nous assurons qu'il s'adapte aux comportements changeants des utilisateurs et aux mises à jour du catalogue.
