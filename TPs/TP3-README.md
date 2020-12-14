#Wikipedia
Recupérez le code depuis le répertoire `code/spark/wikipedia`
Le fichier de données se trouve sour le réperoitre de votre projet `src/main/resources/wikipedia`.

Dans ce tp, vous apprendrez à connaître Spark en explorant les articles de Wikipédia en texte intégral.

Évaluer la popularité d'un langage de programmation est important pour les entreprises qui jugent si elles doivent ou non adopter un langage de programmation émergent.
Pour cette raison, la société d'analystes du secteur RedMonk a calculé deux fois par an un classement de la popularité des langages de programmation à l'aide de diverses sources de données, généralement à partir de sites Web comme GitHub et StackOverflow.

Voir leur classement parmi [les 20 premiers pour juin 2016](http://redmonk.com/sogrady/2016/07/20/language-rankings-6-16/) à titre d'exemple.

Dans ce TP, nous utiliserons nos données en texte intégral de Wikipédia pour produire une métrique rudimentaire de la popularité d'un langage de programmation, dans le but de voir si nos classements basés sur Wikipedia ont un lien avec les classements populaires de Red Monk.

Vous effectuerez cet exercice sur un seul nœud (votre ordinateur portable), mais vous pouvez également vous diriger vers [Databricks Community Edition](https://community.cloud.databricks.com/login.html) pour expérimenter gratuitement votre code sur un «micro-cluster».

###Read-in Wikipedia Data
Nous avons déjà implémenté une méthode **parse** dans l'objet **WikipediaData** qui parse une ligne du dataset et la transforme en **WikipediaArticle**.

Créez un RDD (en implémentant **val wikiRdd**) qui contient les objets **WikipediaArticle d'articles**.


###Calculer un classement des langages de programmation
Nous utiliserons une métrique simple pour déterminer la popularité d'un langage de programmation: le nombre d'articles Wikipédia mentionnant le langage au moins une fois.

#### Classment des langages tentative#1: *RankLang*
#####Computing occurrencesOfLang
Commencez par implémenter une méthode d'aide **occurrencesOfLang** qui calcule le nombre d'articles dans un **RDD** de type **RDD[WikipediaArticles]** qui mentionnent au moins une fois la language donné.

Dans un souci de simplicité, nous vérifions qu'au moins un mot (délimité par des espaces) du texte de l'article est égal au langage donné.

#####Calcul du classement, rankLangs
En utilisant **occurrencesOfLang**, implémentez une méthode **rankLangs** qui calcule une liste de paires où le deuxième composant de la paire est le nombre d'articles qui mentionnent la langage (le premier composant de la paire est le nom du langage).

La liste doit être triée par ordre décroissant. Autrement dit, selon ce classement, la paire avec le deuxième composant le plus élevé (le nombre) devrait être le premier élément de la liste.

Un exemple de ce que **rankLangs** pourrait renvoyer pourrait ressembler à ceci, par exemple:
```scala
List(("Scala", 999999), ("JavaScript", 1278), ("LOLCODE", 982), ("Java", 42))
```
*Faites attention à environ combien de temps il faut pour exécuter cette partie! (Cela devrait prendre des dizaines de secondes.)*

#### Classment des langages tentative#2: *rankLangsUsingIndex*
#####Calculer un index inversé
Un index inversé est une structure de données d'index stockant un mappage d'un contenu, tel que des mots ou des nombres, à un ensemble de documents. En particulier, le but d'un index inversé est de permettre des recherches rapides en texte intégral.
Dans notre cas d'utilisation, un index inversé serait utile pour mapper les noms des langages de programmation à la collection d'articles Wikipédia qui mentionnent le nom au moins une fois.

Pour rendre le travail avec l'ensemble de données plus efficace et plus pratique, implémentez une méthode qui calcule un «index inversé» qui mappe les noms de langage de programmation aux articles Wikipédia sur lesquels ils apparaissent au moins une fois.

Implémentez la méthode **makeIndex** qui renvoie un **RDD** du type suivant: **RDD[(String, Iterable [WikipediaArticle])].**
Ce **RDD** contient des paires, de sorte que pour chaque langage de la liste de **langs**, il y a au plus une paire. De plus, le deuxième composant de chaque paire (l'**Iterable**) contient les **WikipediaArticles** qui mentionnent le langage au moins une fois.

*Astuce: vous pouvez utiliser les méthodes **flatMap** et **groupByKey** sur RDD pour cette partie.*

#####Calcul du classement, rankLangsUsingIndex
Utilisez la méthode **makeIndex** implémentée dans la partie précédente pour implémenter une méthode plus rapide pour calculer le classement des langages.


Comme dans la partie 1, **rankLangsUsingIndex** devrait calculer une liste de paires où le deuxième composant de la paire est le nombre d'articles qui mentionnent le langage (le premier composant de la paire est le nom de le langage).
Encore une fois, la liste doit être triée par ordre décroissant. Autrement dit, selon ce classement, la paire avec le deuxième composant le plus élevé (le nombre) devrait être le premier élément de la liste.

*Astuce: la méthode **mapValues** ​​sur **PairRDD** pourrait être utile pour cette partie.*

**Pouvez-vous remarquer une amélioration des performances par rapport à la tentative n ° 2? Pourquoi?**

#### Classment des langages tentative#3: *rankLangsReduceByKey*
Dans le cas où l'index inversé ci-dessus n'est utilisé que pour le calcul du classement et pour aucune autre tâche (recherche en texte intégral, par exemple), il est plus efficace d'utiliser la méthode **reductionByKey** pour calculer directement le classement, sans calculer au préalable un index inversé.
Notez que la méthode **reductionByKey** n'est définie que pour les **RDD contenant des paires** (chaque paire est interprétée comme une paire clé-valeur).

Implémentez la méthode **rankLangsReduceByKey**, cette fois en calculant le classement sans l'index inversé, à l'aide de **reductionByKey**.

Comme dans les parties 1 et 2, **rankLangsReduceByKey** devrait calculer une liste de paires où le deuxième composant de la paire est le nombre d'articles qui mentionnent le langage (le premier composant de la paire est le nom de le langage).

Encore une fois, la liste doit être triée par ordre décroissant. Autrement dit, selon ce classement, la paire avec le deuxième composant le plus élevé (le nombre) devrait être le premier élément de la liste.

**Pouvez-vous remarquer une amélioration des performances par rapport à la mesure à la fois du calcul de l'indice et du calcul du classement comme nous l'avons fait dans la tentative n ° 2? Si oui, pouvez-vous penser à une raison?**
