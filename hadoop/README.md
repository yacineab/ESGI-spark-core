# Librairie Hadoop pour Windows

Afin de pouvoir utiliser correctement Spark avec le système de fichier Windows il est nécessaire de disposer du binaire winutils.exe.
Ce binaire permet d'ecrire des données depuis Spark le système de fichier Windows

Spark attend ```winutils.exe``` dans l'installation Hadoop "<Répertoire d'installation Hadoop>/bin/winutils.exe" (notez le dossier "bin")

### Création de HADOOP_HOME

- Créer un un répertoire pour les binaire hadoop: e.g: ```C:\tmp\hadoop\bin\```
- Télécharger le fichier [winutils.exe](https://github.com/yacineab/ESGI-spark-core/blob/master/hadoop/bin/winutils.exe "hadoop home")
- Placer le fichier ```winutils.exe``` dans le répertoire ```C:\tmp\hadoop\bin\```

- Télécharger le fichier hadoop.dll depuis le github : [hadoop.dll](https://github.com/yacineab/ESGI-spark-core/blob/master/hadoop/bin/hadoop.dll "Download Hadoop.dll")  
-	Copier le fichier hadoop.dll dans `C:/Windosw/System32`



#### Definir la variable d'environnement ```HADOOP_HOME```

- Sous Windows 10 et Windows 8 accédez à ``` Panneau de configuration> Système> Paramètres système avancés.```
- Windows 7, cliquez avec le bouton droit sur Poste de travail et sélectionnez ``` Propriétés> Avancé.```
- Cliquez sur le bouton Variables d'environnement.
- Sous Variables système, cliquez sur Nouveau.
- Dans le champ Nom de variable, entrez: ```HADOOP_HOME ```
- Dans le champ Valeur , entrez votre chemin du répertoire ```hadoop``` . e.g: ```C:\tmp\hadoop```
- Cliquez sur OK et appliquez les modifications 

![Variables Environnement](https://github.com/yacineab/ESGI-spark-core/blob/master/hadoop/vehadoop.png)
