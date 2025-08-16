import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.io._

object CovidAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Covid19 Analysis")                                                                                      // Ορισμός ονόματος της εφαρμογής Spark
      .master("local[*]")                                                                                               // Εκτέλεση τοπικά, με χρήση όλων των πυρήνων της CPU
      .getOrCreate()                                                                                                    // Δημιουργία ή λήψη υπάρχουσας SparkSession

    import spark.implicits._                                                                                            // Εισαγωγή για να χρησιμοποιήσουμε DataFrame API με $"column"

    val df = spark.read
      .option("header", "true")                                                                                         // Το πρώτο row είναι οι τίτλοι των στηλών
      .option("inferSchema", "true")                                                                                    // Αυτόματη ανίχνευση τύπων δεδομένων
      .option("delimiter", ",")                                                                                         // Ο διαχωριστής πεδίων είναι το κόμμα
      .csv("/home/user/Desktop/Input/covid.csv")                                                                        // Φόρτωση CSV αρχείου με δεδομένα covid

    df.cache()                                                                                                          // Κρατάει το DataFrame προσωρινά στη μνήμη cache για καλύτερη απόδοση
    df.createOrReplaceTempView("covid")                                                                                 // Δημιουργία προσωρινού πίνακα SQL με όνομα "covid"

                                                                                                                        // Δημιουργία writer για ενιαίο output αρχείο
    val outputPath = "/home/user/Desktop/covid_results.txt"                                                             // Διαδρομή εξόδου
    val writer = new PrintWriter(new File(outputPath))                                                                  // Αντικείμενο για εγγραφή σε αρχείο


    def writeResult(title: String, df: org.apache.spark.sql.DataFrame): Unit = {                                        // Συνάρτηση για εγγραφή των αποτελεσμάτων σε μορφή κειμένου
      writer.println("=" * 60)                                                                                          // Εκτύπωση γραμμής διαχωρισμού
      writer.println(s"ΑΠΟΤΕΛΕΣΜΑΤΑ: $title")                                                                           // Τίτλος ερωτήματος με χρήση s-string $title
      writer.println("=" * 60)
      df.collect().foreach(row => writer.println(row.mkString(",  ")))                                                  // Εκτύπωση κάθε γραμμής ως CSV
      writer.println("\n")                                                                                              // Κενή γραμμή για διαχωρισμό ερωτημάτων
    }

    // ------------------------------------------------
    // Ερώτημα 1: Κρούσματα Δεκεμβρίου 2020 στην Ελλάδα
    // ------------------------------------------------

    val decGreeceSQL = spark.sql("""
     SELECT dateRep, cases                                                                                              -- Επιλέγουμε τις στήλες dateRep,Cases
     FROM covid                                                                                                         -- Απο το αρχείο covid
     WHERE countriesAndTerritories = 'Greece' AND month = 12 AND year = 2020                                            -- Επιλογή εγγραφών μόνο για την Ελλάδα & μήνα Δεκέμβριο(μήνα=12) του 2020
     ORDER BY dateRep                                                                                                   -- Ταξινόμηση κατά ημερομηνία
      """)

    println("Ερώτημα 1 - SQL:")                                                                                         // Εμφάνιση αποτελεσμάτων στον πίνακα
    decGreeceSQL.show()
    writeResult("ΕΡΩΤΗΜΑ 1 - SQL: Κρούσματα Δεκεμβρίου 2020 στην Ελλάδα", decGreeceSQL)                                 // Εγγραφή των αποτελεσμάτων στο αρχείο εξόδου


    val decGreeceDF = df
      .filter($"countriesAndTerritories" === "Greece" && $"month" === 12 && $"year" === 2020)                           // Φιλτράρισμα των γραμμών για Ελλάδα, Δεκέμβριο 2020
      .select("dateRep", "cases")                                                                                       // Επιλογή στηλών ημερομηνίας και κρουσμάτων
      .orderBy("dateRep")                                                                                               // Ταξινόμηση κατά ημερομηνία

    println("Ερώτημα 1 - DataFrame:")
    decGreeceDF.show()                                                                                                  // Εμφάνιση των αποτελεσμάτων στην κονσόλα
    writeResult("ΕΡΩΤΗΜΑ 1 - DataFrame: Κρούσματα Δεκεμβρίου 2020 στην Ελλάδα", decGreeceDF)                            // Εγγραφή των αποτελεσμάτων στο αρχείο εξόδου

    // ------------------------------------------------
    // Ερώτημα 2: Σύνολο κρουσμάτων και θανάτων ανά ήπειρο
    // ------------------------------------------------

    val continentTotalsSQL = spark.sql("""
     SELECT continentExp,                                                                                               -- Επιλογή Ηπείρων
             SUM(cases) AS total_cases,
             SUM(deaths) AS total_deaths                                                                                -- Υπολογισμός συνολικών κρουσμάτων και θανάτων
     FROM covid                                                                                                         -- Απο τον αρχείο Covid
     GROUP BY continentExp                                                                                              -- Ομαδοποίηση κατά ήπειρο
     """)

    println("Ερώτημα 2 - SQL:")
    continentTotalsSQL.show()                                                                                           // Εμφάνιση αποτελεσμάτων
    writeResult("ΕΡΩΤΗΜΑ 2 - SQL: Σύνολο κρουσμάτων και θανάτων ανά ήπειρο", continentTotalsSQL)                        // Εγγραφή των αποτελεσμάτων στο αρχείο εξόδου


    val continentTotalsDF = df
      .groupBy("continentExp")                                                                                          // Ομαδοποίηση κατά ήπειρο
      .agg(
        sum("cases").alias("total_cases"),                                                                              // Σύνολο κρουσμάτων
        sum("deaths").alias("total_deaths")                                                                             // Σύνολο θανάτων
      )

    println("Ερώτημα 2 - DataFrame:")
    continentTotalsDF.show()                                                                                            // Εμφάνιση αποτελεσμάτων
    writeResult("ΕΡΩΤΗΜΑ 2 - DataFrame: Σύνολο κρουσμάτων και θανάτων ανά ήπειρο", continentTotalsDF)                   // Εγγραφή των αποτελεσμάτων στο αρχείο εξόδου


    // ------------------------------------------------
    // Ερώτημα 3: Μέσος όρος κρουσμάτων/θανάτων ανά χώρα στην Ευρώπη
    // ------------------------------------------------

    val europeAvgSQL = spark.sql("""
     SELECT countriesAndTerritories,                                                                                    -- Επιλογή χωρών
            ROUND(AVG(cases), 1) AS avg_cases,                                                                          -- Μέσος όρος ανά χώρα με χρήση AVG , στρογγυλοποίηση ανα .1 δεκαδικό ψηφίο και Alias avg_cases
            ROUND(AVG(deaths), 1) AS avg_deaths                                                                         -- Μέσος όρος ανά χώρα με χρήση AVG , στρογγυλοποίηση ανα .1 δεκαδικό ψηφίο και Alias avg_deaths
      FROM covid
      WHERE continentExp = 'Europe'                                                                                     -- Επιλογή δεδομένων μόνο από Ευρώπη
      GROUP BY countriesAndTerritories                                                                                  -- Ταξινόμηση ανά χώρα
      ORDER BY countriesAndTerritories
      """)

    println("Ερώτημα 3 - SQL:")
    europeAvgSQL.show()                                                                                                 // Εμφάνιση αποτελεσμάτων
    writeResult("ΕΡΩΤΗΜΑ 3 - SQL: Μέσος όρος κρουσμάτων/θανάτων ανά χώρα στην Ευρώπη", europeAvgSQL)                    // Εγγραφή των αποτελεσμάτων στο αρχείο εξόδου

    val europeAvgDF = df
      .filter($"continentExp" === "Europe")                                                                             // Φιλτράρισμα μόνο για Ευρώπη
      .groupBy("countriesAndTerritories")                                                                               // Ομαδοποίηση κατά χώρα
      .agg(
        round(avg("cases"), 1).alias("avg_cases"),                                                                      // Μέσος όρος κρουσμάτων (1 δεκαδικό)
        round(avg("deaths"), 1).alias("avg_deaths")                                                                     // Μέσος όρος θανάτων (1 δεκαδικό)
      )
      .orderBy("countriesAndTerritories")                                                                               // Ταξινόμηση αλφαβητικά

    println("Ερώτημα 3 - DataFrame:")
    europeAvgDF.show()
    writeResult("ΕΡΩΤΗΜΑ 3 - DataFrame: Μέσος όρος κρουσμάτων/θανάτων ανά χώρα στην Ευρώπη", europeAvgDF)


    // ------------------------------------------------
    // Ερώτημα 4: 10 χειρότερες ημερομηνίες κρουσμάτων στην Ευρώπη
    // ------------------------------------------------

    val worstDaysSQL = spark.sql("""
    SELECT dateRep, SUM(cases) AS total_cases                                                                           -- Άθροισμα των Cases για κάθε ημερομηνία
    FROM covid                                                                                                          -- Επιλογή φακέλου covid
    WHERE continentExp = 'Europe'                                                                                       -- Επιλογή δεδομένων μόνο από Ευρώπη
    GROUP BY dateRep                                                                                                    -- Ομαδοποίηση ανά ημερομηνία
    ORDER BY total_cases DESC                                                                                           -- Κατά φθίνουσα σειρά
    LIMIT 10                                                                                                            -- Περιορισμός στα 10 χειρότερα
    """)

    println("Ερώτημα 4 - SQL:")
    worstDaysSQL.show()
    writeResult("ΕΡΩΤΗΜΑ 4 - SQL: 10 χειρότερες ημερομηνίες κρουσμάτων στην Ευρώπη", worstDaysSQL)

    val worstDaysDF = df
      .filter($"continentExp" === "Europe")                                                                             // Φιλτράρισμα μόνο για Ευρώπη
      .groupBy("dateRep")                                                                                               // Ομαδοποίηση ανά ημερομηνία
      .agg(sum("cases").alias("total_cases"))                                                                           // Σύνολο κρουσμάτων
      .orderBy(desc("total_cases"))                                                                                     // Φθίνουσα ταξινόμηση
      .limit(10)                                                                                                        // Περιορισμός στα 10 χειρότερα

    println("Ερώτημα 4 - DataFrame:")
    worstDaysDF.show()
    writeResult("ΕΡΩΤΗΜΑ 4 - DataFrame: 10 χειρότερες ημερομηνίες κρουσμάτων στην Ευρώπη", worstDaysDF)

    writer.close()                                                                                                      // Κλείνει τον writer για το αρχείο εξόδου
    spark.stop()                                                                                                        // Σταματάει το SparkSession
  }
}

