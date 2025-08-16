import org.apache.spark.sql.SparkSession                                                                                // Εισαγωγή βιβλιοθήκης SparkSession
import org.apache.spark.sql.functions._                                                                                 // Εισαγωγή βιβλιοθήκης για λειτουργίες Spark
import java.io.PrintWriter                                                                                              // Εισαγωγή βιβλιοθήκης για τη γραφή σε αρχείο

object SalesAnalysis {                                                                                                  // Ορισμός της κύριας κλάσης SalesAnalysis
  def main(args: Array[String]): Unit = {                                                                               // Κύρια συνάρτηση για εκτέλεση του προγράμματος

    val spark = SparkSession.builder()                                                                                  // Δημιουργία SparkSession
      .appName("SalesAnalysis")                                                                                         // Ορισμός του ονόματος του job
      .master("local[*]")                                                                                               // Ορισμός τοπικού cluster
      .getOrCreate()                                                                                                    // Δημιουργία ή ανάκτηση του υπάρχοντος SparkSession

    import spark.implicits._                                                                                            // Εισαγωγή μετατροπής DataFrame σε Dataset

    val df = spark.read.option("header", "true").option("inferSchema", "true")                                          // Ανάγνωση CSV αρχείου με επιλογές την ενεργοποιημένη την αυτόματη ανίχνευση τύπων (inferSchema) και header
      .csv("/home/user/Desktop/Input/sales.csv")                                                                        // Ανάγνωση του αρχείου CSV με τα δεδομένα πωλήσεων

    val salesDF = df.withColumn("TotalCost", $"Quantity" * $"UnitPrice")                                                // Δημιουργία στήλης TotalCost:Quality * UnitPrice
    salesDF.createOrReplaceTempView("sales")                                                                            // Δημιουργία προσωρινής προβολής για SQL ερωτήματα

    val results = new StringBuilder()                                                                                   // Δημιουργία StringBuilder για αποθήκευση αποτελεσμάτων

    def appendResult(title: String, df: org.apache.spark.sql.DataFrame): Unit = {                                       // Συνάρτηση για προσθήκη αποτελεσμάτων
      println(s"\n=== $title ===")                                                                                      // Εκτύπωση τίτλου στην κονσόλα
      df.show(false)                                                                                                    // Εκτύπωση των δεδομένων του DataFrame στην κονσόλα χωρίς περικοπές (false)
      results.append(s"\n=== $title ===\n")                                                                             // Προσθήκη τίτλου στο αποτέλεσμα
      val rows = df.collect().map(_.mkString(","))                                                                      // Συλλογή των δεδομένων ως String με διαχωρισμό,
      rows.foreach(r => results.append(  + "\n"))                                                                       // Προσθήκη των γραμμών στο StringBuilder
    }

    // ========================= Ερώτημα 1: Κορυφαία 5 τιμολόγια με τη μεγαλύτερη αξία =========================
    // =========================================================================================================

    val topInvoicesDF = salesDF
      .groupBy("InvoiceNo")                                                                                             // Ομαδοποίηση κατά InvoiceNo
      .agg(round(sum("TotalCost"), 2).alias("InvoiceTotal"))                                                            // Άθροισμα της τιμής των προϊόντων
      .orderBy(desc("InvoiceTotal"))                                                                                    // Ταξινόμηση κατά το συνολικό ποσό
      .limit(5)                                                                                                         // Λήψη των 5 πρώτων
    appendResult("Top 5 Invoices (DataFrame)", topInvoicesDF)                                                           // Εμφάνιση αποτελεσμάτων

    val topInvoicesSQL = spark.sql(                                                                                     // Εκτέλεση του SQL ερωτήματος για τα κορυφαία 5 τιμολόγια
      """SELECT InvoiceNo, ROUND(SUM(TotalCost), 2) AS InvoiceTotal                                                     -- Διαλέγουμε τo SUM(Total Cost) απο το InvoiceNo και στρογγυλοποίηση σε 2 δεκαδικά
         FROM sales
         GROUP BY InvoiceNo
         ORDER BY InvoiceTotal DESC
         LIMIT 5""")
    appendResult("Top 5 Invoices (SQL)", topInvoicesSQL)                                                                // Εμφάνιση αποτελεσμάτων SQL

    // ========================= Ερώτημα 2: Τα πιο πωληθέντα προϊόντα ==========================================
    // =========================================================================================================

    val popularProductsDF = salesDF
      .groupBy("Description")                                                                                           // Ομαδοποίηση κατά περιγραφή προϊόντος
      .agg(sum("Quantity").alias("TotalSold"))                                                                          // Άθροισμα των ποσοτήτων για κάθε προϊόν
      .orderBy(desc("TotalSold"))                                                                                       // Ταξινόμηση κατά τις ποσότητες πωλήσεων
      .limit(5)                                                                                                         // Λήψη των 5 πρώτων
    appendResult("Most Sold Products (DataFrame)", popularProductsDF)                                                   // Εμφάνιση αποτελεσμάτων

    val popularProductsSQL = spark.sql(                                                                                 // Εκτέλεση του SQL ερωτήματος για τα πιο πωληθέντα προϊόντα
      """SELECT Description, SUM(Quantity) AS TotalSold
         FROM sales
         GROUP BY Description
         ORDER BY TotalSold DESC
         LIMIT 5""")
    appendResult("Most Sold Products (SQL)", popularProductsSQL)                                                        // Εμφάνιση αποτελεσμάτων SQL

    // ========================= Ερώτημα 3: Τα πιο συχνά τιμολογημένα προϊόντα =================================
    // =========================================================================================================

    val frequentProductsDF = salesDF
      .select("InvoiceNo", "Description")                                                                               // Επιλογή στήλης για τιμολόγια και περιγραφές
      .distinct()                                                                                                       // Αφαίρεση επαναλήψεων (distinct=μοναδικότητα)
      .groupBy("Description")                                                                                           // Ομαδοποίηση κατά περιγραφή προϊόντος
      .agg(count("*").alias("InvoiceCount"))                                                                            // Καταμέτρηση των μοναδικών τιμολογίων
      .orderBy(desc("InvoiceCount"))                                                                                    // Ταξινόμηση κατά αριθμό τιμολογίων
      .limit(5)                                                                                                         // Λήψη των 5 πρώτων
    appendResult("Most Frequently Invoiced Products (DataFrame)", frequentProductsDF)                                   // Εμφάνιση αποτελεσμάτων

    val frequentProductsSQL = spark.sql(                                                                                // Εκτέλεση του SQL ερωτήματος για τα πιο συχνά τιμολογημένα προϊόντα
      """SELECT Description, COUNT(DISTINCT InvoiceNo) AS InvoiceCount
         FROM sales
         GROUP BY Description
         ORDER BY InvoiceCount DESC
         LIMIT 5""")
    appendResult("Most Frequently Invoiced Products (SQL)", frequentProductsSQL)                                        // Εμφάνιση αποτελεσμάτων SQL

    // ========================= Ερώτημα 4: Μέσος όρος προϊόντων και κόστους ανά τιμολόγιο =========================
    // =============================================================================================================

    val avgPerInvoiceDF = salesDF
      .groupBy("InvoiceNo")                                                                                             // Ομαδοποίηση κατά InvoiceNo
      .agg(
        sum("Quantity").alias("TotalItems"),                                                                            // Άθροισμα των προϊόντων ανά τιμολόγιο
        sum("TotalCost").alias("InvoiceTotal")                                                                          // Άθροισμα του κόστους ανά τιμολόγιο
      )
      .agg(
        round(avg("TotalItems"), 2).alias("AvgItemsPerInvoice"),                                                        // Υπολογισμός μέσου όρου προϊόντων ανά τιμολόγιο
        round(avg("InvoiceTotal"), 2).alias("AvgCostPerInvoice")                                                        // Υπολογισμός μέσου όρου κόστους ανά τιμολόγιο
      )
    appendResult("Average Items and Cost Per Invoice (DataFrame)", avgPerInvoiceDF)                                     // Εμφάνιση αποτελεσμάτων

    val avgPerInvoiceSQL = spark.sql(                                                                                   // Εκτέλεση του SQL ερωτήματος για μέσο όρο προϊόντων και κόστους ανά τιμολόγιο
      """WITH InvoiceAgg AS (
           SELECT InvoiceNo, SUM(Quantity) AS TotalItems, SUM(TotalCost) AS InvoiceTotal
           FROM sales
           GROUP BY InvoiceNo
         )
         SELECT ROUND(AVG(TotalItems), 2) AS AvgItemsPerInvoice,
                ROUND(AVG(InvoiceTotal), 2) AS AvgCostPerInvoice
         FROM InvoiceAgg""")
    appendResult("Average Items and Cost Per Invoice (SQL)", avgPerInvoiceSQL)                                          // Εμφάνιση αποτελεσμάτων SQL

    // ========================= Ερώτημα 5: Κορυφαίοι πελάτες με τη μεγαλύτερη αξία αγοράς =========================
    // =============================================================================================================

    val topCustomersDF = salesDF
      .filter($"CustomerID".isNotNull)                                                                                  // Φιλτράρισμα για πελάτες που δεν είναι null
      .groupBy("CustomerID")                                                                                            // Ομαδοποίηση κατά CustomerID
      .agg(round(sum("TotalCost"), 2).alias("Revenue"))                                                                 // Υπολογισμός συνολικών εσόδων ανά πελάτη
      .orderBy(desc("Revenue"))                                                                                         // Ταξινόμηση κατά έσοδα DESC
      .limit(5)                                                                                                         // Λήψη των 5 πρώτων
    appendResult("Top Customers (DataFrame)", topCustomersDF)                                                           // Εμφάνιση αποτελεσμάτων

    val topCustomersSQL = spark.sql(                                                                                    // Εκτέλεση του SQL ερωτήματος για τους κορυφαίους πελάτες
      """SELECT CustomerID, ROUND(SUM(TotalCost), 2) AS Revenue
         FROM sales
         WHERE CustomerID IS NOT NULL
         GROUP BY CustomerID
         ORDER BY Revenue DESC
         LIMIT 5""")
    appendResult("Top Customers (SQL)", topCustomersSQL)                                                                // Εμφάνιση αποτελεσμάτων SQL

                                                                                                                        // Γράφουμε τα πάντα σε ένα αρχείο
    val writer = new PrintWriter("/home/user/Desktop/sales_output.txt")                                                 // Δημιουργία PrintWriter για το αρχείο εξόδου
    writer.write(results.toString())                                                                                    // Γράφουμε το περιεχόμενο στο αρχείο
    writer.close()                                                                                                      // Κλείσιμο του PrintWriter

    spark.stop()                                                                                                        // Τερματισμός της συνεδρίας Spark
  }
}
