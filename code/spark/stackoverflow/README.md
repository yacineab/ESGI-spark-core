# StackOverflow
Pour commencer récupérer le projet depuis le git (code/spark/stackoverflow) et importer le projet dans votre IDE.

Pour ce TP vous devez également télécharger les données (170 Mo): [StackOverflow-Tp-DataSet](https://we.tl/t-qAygJZ8FBC)
et placez-le dans le dossier: `src/main/resources/stackoverflow` dans le répertoire de votre projet.

L'objectif global de cette mission est de mettre en œuvre un algorithme de **`k-means distribué`** qui regroupe les publications sur la plate-forme de questions-réponses populaire **StackOverflow** en fonction de leur score. De plus, ce clustering doit être exécuté en parallèle pour différents langages de programmation, et les résultats doivent être comparés.

## La motivation est la suivante
**StackOverflow** est une source importante de documentation.
Cependant, différentes réponses fournies par les utilisateurs peuvent avoir des notes très différentes (basées sur les votes des utilisateurs) en fonction de leur valeur perçue.
Par conséquent, nous aimerions examiner la répartition des questions et leurs réponses.

Par exemple, combien de réponses bien notées les utilisateurs de StackOverflow publient-ils et quel est leur score? Y a-t-il de grandes différences entre les réponses les mieux notées et les moins bien notées?

Enfin, nous souhaitons comparer ces distributions pour différentes communautés de langage de programmation.
Les différences dans les distributions pourraient refléter des différences dans la disponibilité de la documentation. Par exemple, StackOverflow pourrait avoir une meilleure documentation pour une certaine bibliothèque que la documentation API de cette bibliothèque.
Cependant, pour éviter des conclusions invalides, nous nous concentrerons sur le problème bien défini du regroupement des réponses en fonction de leurs scores.

*Si vous ne connaissiez pas l'algorithme k-means, vous pouvez lire cette page [wikipedia](https://fr.wikipedia.org/wiki/K-moyennes)*

## The Data

Vous recevez un fichier CSV (valeurs séparées par des virgules) contenant des informations sur les publications StackOverflow. Chaque ligne du fichier texte fourni a le format suivant:
```
<postTypeId>,<id>,[<acceptedAnswer>],[<parentId>],<score>,[<tag>]
```
Une brève explication des champs séparés par des virgules:

```
<postTypeId>: Type de publication. Type 1 = question,
                   type 2 = réponse.

<id>: identifiant unique du message (quel que soit le type).

<acceptedAnswer>: ID du message de réponse accepté. Ce
                   les informations sont facultatives, donc peut-être manquantes
                   indiqué par une chaîne vide.

<parentId>: Pour une réponse: id du correspondant
                   question. Pour une question: manquant, indiqué
                   par une chaîne vide.

<score>: le score StackOverflow (basé sur l'utilisateur
                   votes).

<tag>: La balise indique le langage de programmation
                   dont parle le message, au cas où il s'agirait d'un
                   question, ou manquant au cas où ce serait une réponse.
```

## Data Processing
Nous allons maintenant voir comment vous traitez les données avant d'appliquer l'algorithme kmeans.

Les structures des méthodes sont définies pour vous dans les deux classes `StackOverflow` et `StackOverflowUtils`. Afin de répondre aux questions vous devriez remplacer les `???` par votre code.

#### Lecture de données
**1. La premiere chose à faire est d'instancier le **SparkSession**, pour cela completez le code:**

```scala
// Defining the spark Session
   val spark = ???
```

**2. Charger votre fichier csv dans un DataFrame**

**3. Donnez un nom aux colonnes de votre DataFrame, correspondant à la description plus haut.**

Nous souhaitons avoir sur la meme ligne du DataFrame la question et sa réponse ayant obtenu le meilleure score.

Pour obtenir cela, dans la méthode groupedPostings, filtrez d'abord les questions et les réponses séparément. Ensuite, utilisez l'une des opérations de jointure (laquelle?) Pour obtenir un DataFrame avec les colonnes [QID, Question, Answer].
Ensuite, la dernière étape consiste à obtenir un DataFrame [Question, max(Answers.score)].

*hint: Utilisez un `groupBy` couplé à une fonction d'aggrégation sur le max du score sur les réponses*
Enfin, dans la description, nous avons utilisé les types `QID`, `Question` et `Answer`, que nous avons définis comme des alias de type pour les `Posting`s et les `Ints`. La liste complète des alias de type est disponible dans `package.scala`:

```Scala
package object stackoverflow {
  type Question = Posting
  type Answer = Posting
  type QID = Int
  type HighScore = Int
  type LangIndex = Int
}
```

**4. Implementer la méthode `groupedPostings`**
Cette méthode devrait retourner un DataFrame avec le schema:
```shell
root
 |-- postTypeId: integer (nullable = true)
 |-- id: integer (nullable = true)
 |-- acceptedAnswer: string (nullable = true)
 |-- parentId: integer (nullable = true)
 |-- score: integer (nullable = true)
 |-- tag: string (nullable = true)
 |-- answer_maxscore: string (nullable = true)
```
**5. A partir du DataFrame précédent, Définissez un vecteur de type DataSet[(Question, Int)] contenant une question et le score de réponse maximum**
La signature de ce DataSet est :
```scala
val vectorsDS: Dataset[(Question, Int)]
```

**Remarque**
Afin de transformer un DataFrame en DataSet nous avons besoin d'encodeurs, d'où l'import ci-dessous:
```Scala    
import spark.implicits._
```


### Création de vecteurs pour le clustering

Ensuite, nous préparons l'entrée pour l'algorithme de clustering. Pour cela, nous transformons le DataSet noté en un vecteur DataSet contenant les vecteurs à regrouper. Dans notre cas, les vecteurs doivent être des paires avec deux composants (dans l'ordre indiqué!):

- Index du langage (dans la liste langs) multiplié par le facteur langSpread.
- Le score de réponse le plus élevé (calculé ci-dessus).

Le facteur **langSpread** est fourni (défini sur 50000).
Fondamentalement, il s'assure que les publications sur différents langages de programmation ont au moins une distance de 50000 en utilisant la mesure de distance fournie par la fonction euclideanDist. Vous apprendrez plus tard ce que signifie cette distance et pourquoi elle est réglée sur cette valeur.

Le type des vecteurs DataSet est le suivant:
```Scala
def vectorPostings(scored: Dataset[(Posting, HighScore)]): Dataset[(LangIndex, HighScore)] = ???
```
Par exemple, les vecteurs DataSets doivent contenir les tuples suivants:
```Scala
(350000, 67)
(100000, 89)
(300000, 3)
(50000,  30)
(200000, 20)
```

**6. Implémentez cette fonctionnalité dans la méthode `vectorPostings` à l'aide de la méthode d'assistance `firstLangInTag` donnée.**
*(Pour le test: le DataSet noté doit avoir 2121822 entrées)*

#### Kmeans Clustering

```scala
 val means = kmeans(sampleVectors(vectors), vectors)
```
Sur la base de ces moyens initiaux et de la méthode de convergence des variables fournies, implémentez l'algorithme K-means de manière itérative.
- appariement de chaque vecteur avec l'indice de la moyenne la plus proche (son cluster);
- calculer les nouveaux moyens en faisant la moyenne des valeurs de chaque cluster.

Pour implémenter ces étapes itératives, utilisez les fonctions fournies `findClosest, averageVectors et euclideanDistance`.

**Note 1:**
Dans nos tests, la convergence est atteinte après 44 itérations (pour langSpread = 50000) et en 104 itérations (pour langSpread = 1), et pour les premières itérations, la distance n'a cessé de croître.
Bien qu'il puisse sembler que quelque chose ne va pas, c'est le comportement attendu. Avoir de nombreux points distants oblige les noyaux à se déplacer un peu et à chaque changement les effets se répercutent sur d'autres noyaux, qui se déplacent également, et ainsi de suite. Soyez patient, en 44 itérations, la distance passera de plus de 100 000 à 13, satisfaisant la condition de convergence.

Si vous souhaitez obtenir les résultats plus rapidement, n'hésitez pas à sous-échantillonner les données (chaque itération est plus rapide, mais il faut encore environ 40 étapes pour converger):
```scala
val scored = scoredPostings(grouped).sample(true, 0.1, 0)
```

**Note 2:**
La variable **langSpread** correspond à la distance qui sépare les langages du point de vue de l'algorithme de clustering. Pour une valeur de 50000, les langages sont trop loin pour être regroupées du tout, ce qui entraîne un clustering qui ne prend en compte que les scores pour chaque langage (de la même manière que le partitionnement des données entre les langages, puis le clustering en fonction du score). Un clustering plus intéressant (mais moins scientifique) se produit lorsque langSpread est défini sur 1 (nous ne pouvons pas le définir sur 0, car il perd complètement les informations de langage), où nous groupons en fonction du score.

#### Computing Cluster Details
```scala
val results = clusterResults(means, vectors)
printResults(results)
```

**7. Implémentez la méthode clusterResults, qui, pour chaque cluster, calcule:**

(a) le langage de programmation dominant dans le cluster;
(b) le pourcentage de réponses appartenant à la langue dominante;
(c) la taille du cluster (le nombre de questions qu'il contient);
(d) la médiane des scores de réponse les plus élevés.
Une fois cette valeur renvoyée, elle est imprimée à l'écran par la méthode printResults.

#### Des questions
1. Pensez-vous que partitionner vos données aiderait?
2. Avez-vous pensé à persister certains de vos DataSet? Pouvez-vous penser aux raisons pour lesquelles persister  vos DataSet en mémoire peut être utile pour cet algorithme?
3. Parmi les clusters non vides, combien de clusters ont «Java» comme étiquette (sur la base de la majorité des questions, voir ci-dessus)? Pourquoi?
4. En considérant uniquement les "clusters Java", quels clusters se démarquent et pourquoi?
5. En quoi les "clusters C #" sont-ils différents des "clusters Java"?
