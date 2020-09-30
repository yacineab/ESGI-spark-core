object SimpleFunctions extends App {

  // definition Variable Scala
  val num = 2 // immutable - evaluated immediately
  var age = 10 // mutable - evaluated immediately
  def size: Int = 2  // evaluated when called
  lazy val taille = 175 // evaluated once when needed

  // print les variables précedentes:
  println(
      "num : " + num + "\n" +
      "age : " + age + "\n" +
      "taille : " + taille + "\n"
  )

  // Calcul du reste de la division Euclidienne a et b
  def euclid(a:Int, b:Int): Int = a%b

  // Calcul max de a et b
  def max(a:Int, b:Int): Int = if(a>b) a else b

  // Calcul carré d'un nombre
  def square(x: Double): Double = x*x

  //Calcul la somme des carrées
  def sumOfSquare(x:Double, y: Double): Double = square(x) + square(y)

}
