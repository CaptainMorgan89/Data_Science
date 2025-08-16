import org.apache.spark.sql.SparkSession

object ChessGames {

  def main(args: Array[String]): Unit = {

    // Δημιουργία SparkSession
    val ss = SparkSession.builder()
      .master("local")                                         // Εκτέλεση τοπικά
      .appName("Chess Games")                                  // Όνομα της εφαρμογής
      .getOrCreate()                                           // Δημιουργία ή φόρτωση του SparkSession


    val fileName = "/home/user/Desktop/Input/games.csv"        // Διαδρομή προς το αρχείο εισόδου


    val gamesRDD = ss.sparkContext.textFile(fileName)          // Ανάγνωση του αρχείου CSV και μετατροπή του σε RDD


    val playerPairs = gamesRDD                                 // Δημιουργία RDD με τα ζευγάρια παικτών και τον αριθμό των παιχνιδιών που έχουν παίξει μαζί
      .map(line => {
        val columns = line.split(",")                          // Χωρισμός της κάθε γραμμής με κόμμα
        val whitePlayer = columns(8)                           // White Player ID
        val blackPlayer = columns(10)                          // Black Player ID


        val pair1 = (whitePlayer, blackPlayer)                 // Δημιουργία των ζευγαριών παικτών (με τρόπο ώστε να μην εξαρτάται από τη σειρά των παικτών)
        val pair2 = (blackPlayer, whitePlayer)


        Seq(pair1, pair2)                                       // Επιστροφή και των δύο ζευγαριών για να είναι ανεξάρτητο από τη σειρά
      })
      .flatMap(x => x)                                          // Ξεδιπλώνουμε τη λίστα των ζευγαριών σε ξεχωριστά στοιχεία ώστε να είναι πιο εύκολο
      .map(pair => (pair, 1))                                   // Κάνουμε map για να έχουμε ζευγάρια και αριθμό 1 για κάθε παιχνίδι
      .reduceByKey((x, y) => x + y)                             // Συγκεντρώνουμε τα αποτελέσματα για να βρούμε το συνολικό αριθμό παιχνιδιών για κάθε ζευγάρι


    val filteredPairs = playerPairs                             // Φιλτράρισμα ζευγαριών που έχουν παίξει πάνω από 5 φορές
      .filter(pair => pair._2 > 5)                              // Ελέγχουμε αν το πλήθος είναι μεγαλύτερο από 5  για το value (._2)


    val sortedPairs = filteredPairs                             // Ταξινόμηση με φθίνουσα σειρά βάσει του αριθμού των παιχνιδιών
      .sortBy(pair => pair._2, ascending = false)


    val outputPath = "/home/user/Desktop/player_pairs_output.txt"          // Αποθήκευση των αποτελεσμάτων σε αρχείο κειμένου στην επιφάνεια εργασίας
    sortedPairs
      .map(pair => s"${pair._1._1} and ${pair._1._2}: ${pair._2} games")   // Δημιουργία της μορφής εξόδου key-value
      .saveAsTextFile(outputPath)                                          // Αποθήκευση σε αρχείο


    sortedPairs                                                               // Εκτύπωση των αποτελεσμάτων στην κονσόλα
      .map(pair => s"${pair._1._1} and ${pair._1._2}: ${pair._2} games")      // Δημιουργία της μορφής εξόδου ((String, String), Int)
      .collect()                                                              // Συλλογή των αποτελεσμάτων στην τοπική μνήμη
      .foreach(println)                                                       // Εκτύπωση κάθε αποτελέσματος στην κονσόλα


    ss.stop()                                                                 // Κλείσιμο του SparkSession
  }
}
