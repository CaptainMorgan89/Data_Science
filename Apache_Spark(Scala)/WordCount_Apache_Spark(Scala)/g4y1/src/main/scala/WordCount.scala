import org.apache.spark.sql.SparkSession
import java.io.PrintWriter

object WordCount {

  def main(args: Array[String]): Unit = {

    // Δημιουργία SparkSession
    val ss = SparkSession.builder()
      .master("local")                               // Τρέξε Spark στον τρέχοντα υπολογιστή
      .appName("WordCount")                          // Δίνει ένα όνομα στην εφαρμογή
      .getOrCreate()                                 // Δημιουργεί νέο SparkSession αν δεν υπάρχει ήδη, αλλιώς επιστρέφει το υπάρχον

    // Διαδρομή προς το αρχείο εισόδου
    val fileName = "/home/user/Desktop/Input/SherlockHolmes.txt"

                                                     // Ανάγνωση και επεξεργασία των λέξεων για καθαρισμό
    val wordsRDD = ss.sparkContext
      .textFile(fileName)                            // Ανάγνωση γραμμών
      .map(x => x.replaceAll("\\p{Punct}", " "))     // Αφαίρεση σημείων στίξης .,!?;:'"()[]{}<>@#$%^&*-_+=|\\/~`€£¢•^•
      .map(x => x.toLowerCase())                     // Μετατροπή σε πεζά
      .flatMap(x => x.split(" "))                    // Διαχωρισμός σε λέξεις
      .map(x => x.trim())                            // Αφαίρεση περιττών κενών
      .filter(x => x.nonEmpty)                       // Αφαίρεση κενών strings σε λίστες ή άλλες συλλογές
      .filter(x => !x.matches("^\\d.*"))             // Αφαίρεση λέξεων που ξεκινούν με αριθμό

    val wordCount = wordsRDD.count()                        // Συνολικός αριθμός λέξεων
    val totalLength = wordsRDD.map(x => x.length).sum()     // Συνολικό μήκος λέξεων με χρήση Lambda
    val averageLength = totalLength / wordCount.toFloat     // Μέσο μήκος λέξης

    println(s"Total word count: $wordCount")                //Χρήση s-string για εκτύπωση μέσα στον κορμό της Print
    println(s"Average word length: $averageLength")


    val wordCounts = wordsRDD                                // Υπολογισμός συχνοτήτων εμφάνισης
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)


    val filteredWords = wordCounts                          // Φιλτράρισμα λέξεων με μήκος > μέσο όρο
      .filter(x => x._1.length > averageLength)             // Παίρνουμε το 1ο στοιχείο (Key-Word) του Tuple χ._1  (key,value)


    val top5Words = filteredWords                           // Εύρεση 5 πιο συχνών λέξεων
      .sortBy(x => x._2, ascending = false)                 // Παίρνουμε το 2ο στοιχείο Value του tuple χ._2 (πλήθος εμφανίσεων)
      .take(5)                                              // Ascending= false δηλαδή Descending= true

    // Εκτύπωση αποτελεσμάτων στην κονσόλα
    println("Top 5 most frequent words with length greater than average:")
    top5Words.foreach(x => println(s"${x._1}: ${x._2}"))                           //Εκτυπώνουμε τα Key-Value του Tuple με χρήση χ lambda expression

    // Αποθήκευση σε αρχείο
    val writer = new PrintWriter("/home/user/Desktop/word_count_results2.txt")
    writer.println(s"Total word count: $wordCount")
    writer.println(s"Average word length: $averageLength")
    writer.println("Top 5 most frequent words with length greater than average:")
    top5Words.foreach(x => writer.println(s"${x._1}: ${x._2}"))
    writer.close()

    ss.stop() // Τερματισμός
  }
}
