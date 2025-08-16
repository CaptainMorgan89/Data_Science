import org.apache.spark.sql.SparkSession

object ChessGames2 {

  def main(args: Array[String]): Unit = {

    // Δημιουργία SparkSession
    val ss = SparkSession.builder()
      .master("local")                                         // Εκτέλεση τοπικά
      .appName("Chess Games")                                  // Όνομα της εφαρμογής
      .getOrCreate()                                           // Δημιουργία ή φόρτωση του SparkSession


    val fileName = "/home/user/Desktop/Input/games.csv"        // Διαδρομή προς το αρχείο εισόδου


    val gamesRDD = ss.sparkContext.textFile(fileName)          // Ανάγνωση του αρχείου CSV σε RDD


    val allMoves = gamesRDD                                    // Δημιουργία RDD με τις κινήσεις κάθε παιχνιδιού
      .map(line => {
        val columns = line.split(",")                          // Χωρισμός της κάθε γραμμής με κόμμα
        val moves = columns(12)                                // All Moves στην στήλη 13
        moves.split(" ")                                       // Διαχωρισμός των κινήσεων σε κάθε παιχνίδι με βάση το κενό που έχει
      })
      .flatMap(moves => moves)                                 // Εξαπλώνουμε τη λίστα των κινήσεων σε ξεχωριστά στοιχεία
      .map(move => (move, 1))                                  // Κάνουμε map για να έχουμε κίνηση και αριθμό 1 για κάθε εμφάνιση
      .reduceByKey((x, y) => x + y)                            // Συγκεντρώνουμε τα αποτελέσματα για να βρούμε το συνολικό αριθμό εμφανίσεων για κάθε κίνηση


    val sortedMoves = allMoves                                 // Ταξινόμηση των κινήσεων με βάση τη συχνότητα εμφάνισης (φθίνουσα σειρά)
      .sortBy(pair => pair._2, ascending = false)              // Ταξινομούμε σε φθίνουσα σειρά

    // Επιλογή των 5 πιο κοινών κινήσεων
    val top5Moves = sortedMoves.take(5)                        // Παίρνουμε τις 5 πρώτες κινήσεις


    val outputPath = "/home/user/Desktop/top_5_moves.txt"      // Αποθήκευση των αποτελεσμάτων σε αρχείο κειμένου στην επιφάνεια εργασίας
    ss.sparkContext.parallelize(top5Moves)
      .map(pair => s"${pair._1},${pair._2}")                   // Δημιουργία της μορφής εξόδου
      .saveAsTextFile(outputPath)                              // Αποθήκευση σε αρχείο


    top5Moves                                                  // Εκτύπωση των 5 πιο κοινών κινήσεων στην κονσόλα
      .map(pair => s"${pair._1},${pair._2}")                   // Δημιουργία της μορφής εξόδου
      .foreach(println)                                        // Εκτύπωση κάθε αποτελέσματος στην κονσόλα


    ss.stop()                                                  // Κλείσιμο του SparkSession
  }
}
