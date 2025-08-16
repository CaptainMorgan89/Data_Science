import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.HashSet;

public class DataCleaner {

    // Μέθοδος για την επεξεργασία της γραμμής του CSV
    private static String[] processLine(String line) {
        // Χρησιμοποιούμε την κανονική έκφραση για να διαχωρίσουμε σωστά τα πεδία
        String regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";                                                             //Ξεκινάμε τη δήλωση μιας κανονικής έκφρασης για να βρούμε το κόμμα (,),

        //  (?:[      Δημιουργούμε μια ομάδα που δεν αποθηκεύει αποτελέσματα. Χρησιμοποιούμε αυτό για να ελέγξουμε το περιεχόμενο χωρίς να αποθηκεύσουμε το αποτέλεσμα για μελλοντική χρήση.
        //  [^\"]*    Αντιστοιχούμε οποιονδήποτε αριθμό χαρακτήρων εκτός από τα εισαγωγικά ("), προκειμένου να βρούμε τα περιεχόμενα μέχρι να συναντήσουμε εισαγωγικό.
        //  \"        Αντιστοιχούμε το εισαγωγικό χαρακτήρα ("), το οποίο χρησιμοποιείται για να περικλείει πεδία με κόμματα μέσα σε αυτά.
        //  [^\"]*    Και πάλι, αντιστοιχούμε οποιονδήποτε αριθμό χαρακτήρων εκτός από εισαγωγικά ", για να καταγράψουμε το περιεχόμενο που βρίσκεται μέσα στα εισαγωγικά.
        //   )        Κλείνουμε τη μη-απορροφητική ομάδα που ελέγχει αν το περιεχόμενο είναι ανάμεσα σε εισαγωγικά.
        //  *         Επιτρέπουμε την επανάληψη αυτής της ομάδας 0 ή περισσότερες φορές, για να καλύψουμε το ενδεχόμενο ότι υπάρχουν πολλά πεδία με κόμματα μέσα σε εισαγωγικά.
        //  [^\"]*    Αντιστοιχούμε οποιονδήποτε αριθμό χαρακτήρων εκτός από εισαγωγικά, μέχρι το τέλος της γραμμής. Αυτή η γραμμή εξασφαλίζει ότι το περιεχόμενο μετά το τελευταίο εισαγωγικό δεν περιέχει κόμμα.
        //   )        Κλείνουμε τη μη-απορροφητική ομάδα.
        //   $)       Ολοκληρώνουμε την θετική αναδρομή (lookahead), που σημαίνει ότι μετά το κόμμα (,) πρέπει να ακολουθεί κάτι που πληροί τις παραπάνω συνθήκες, και πρέπει να φτάσουμε στο τέλος της γραμμής χωρίς να υπάρχει κόμμα μέσα σε εισαγωγικά.





        String[] tokens = line.split(regex, -1);                                                                        // Διαχωρισμός της γραμμής σε πεδία


        for (int i = 0; i < tokens.length; i++) {                                                                       // Αφαίρεση εισαγωγικών από τα πεδία
            tokens[i] = tokens[i].trim().replaceAll("^\"|\"$", "");                                                     // Καθαρισμός εισαγωγικών στην αρχή και στο τέλος της συμβολοσειράς
        }


        if (tokens.length != 9) {                                                                                       // Επιστρέφουμε τα πεδία, αν το μέγεθος τους είναι σωστό (9 πεδία για CSV)
            System.err.println("Αγνοούμε τη γραμμή λόγω λάθους αριθμού στηλών (πρέπει να είναι 9): " + line);
            return null;                                                                                                // Επιστρέφουμε null αν δεν είναι 9 πεδία
        }
        return tokens;                                                                                                  // Επιστρέφουμε τα πεδία
    }


    public static class IMDBMapper extends Mapper<Object, Text, Text, Text> {                                           // Ο Mapper διαβάζει τις γραμμές του αρχείου και εξάγει τα δεδομένα
        private final Text resultKey = new Text();                                                                      // Κλειδί εξόδου (imdbID)
        private final Text resultValue = new Text();                                                                    // Τιμή εξόδου


        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {             // Η μέθοδος map() επεξεργάζεται κάθε γραμμή του αρχείου εισόδου
            String line = value.toString().trim().replaceAll("\t+", "");                                                // Καθαρισμός των tabs


            if (line.startsWith("imdbID")) {                                                                            // Αγνοούμε την επικεφαλίδα του CSV
                return;                                                                                                 // Αν είναι η επικεφαλίδα, επιστρέφουμε χωρίς να κάνουμε τίποτα
            }


            String[] columns = processLine(line);                                                                       // Επεξεργασία της γραμμής και διαχωρισμός των πεδίων
            if (columns == null) {
                return;                                                                                                 // Αν η γραμμή έχει σφάλματα, την αγνοούμε
            }

            // Ανάκτηση των πεδίων από το CSV
            String imdbID = columns[0].trim();
            String title = columns[1].trim();
            String year = columns[2].trim();
            String runtime = columns[3].trim();
            String genre = columns[4].trim();
            String released = columns[5].trim();
            String imdbRating = columns[6].trim();
            String imdbVotes = columns[7].trim();
            String country = columns[8].trim();


            country = country.replaceAll("\\s*,\\s*", " & ");                                                           // Καθαρισμός του country και genre για να ενοποιηθούν τα κόμματα σε "&"
            genre = genre.replaceAll("\\s*,\\s*", " & ");

                                                                                                                        // Δημιουργία κλειδιού και τιμής για το Hadoop Output
            resultKey.set(imdbID);
            resultValue.set(", " + title + ", " + year + ", " + runtime + ", " + genre + ", " + released + ", " + imdbRating + ", " + imdbVotes + ", " + country);


            context.write(resultKey, resultValue);                                                                      // Εγγραφή στο context για το Hadoop
        }
    }


    public static class IMDBReducer extends Reducer<Text, Text, Text, Text> {                                           // Ο Reducer συγκεντρώνει τα αποτελέσματα και αποφεύγει διπλότυπα
        private boolean headerWritten = false;                                                                          // Έλεγχος αν έχουμε ήδη γράψει την επικεφαλίδα
        private HashSet<String> seenIdsAndYears = new HashSet<>();                                                      // Για να αποφεύγουμε διπλότυπα (imdbID, year)

                                                                                                                        // Η μέθοδος reduce() επεξεργάζεται τα δεδομένα που παρέχει ο Mapper
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
                                                                                                                        // Αν δεν έχουμε ήδη γράψει την επικεφαλίδα, την γράφουμε
            if (!headerWritten) {
                context.write(new Text("imdbID,title,year,runtime,genre,released,imdbRating,imdbVotes,country"), new Text());
                headerWritten = true;                                                                                   // Σημειώνουμε ότι γράψαμε την επικεφαλίδα
            }

                                                                                                                        // Επεξεργασία κάθε value που ήρθε από τον Mapper
            for (Text value : values) {
                String[] valueColumns = value.toString().split(",");                                                    // Διαχωρισμός της τιμής από τα πεδία

                if (valueColumns.length >= 3) {
                    String year = valueColumns[2].trim();                                                               // Ανάκτηση του έτους
                    String combinedKey = key.toString() + "," + year;                                                   // Συνδυασμένο κλειδί (imdbID, year)


                    if (!seenIdsAndYears.contains(combinedKey)) {                                                       // Αν δεν έχουμε επεξεργαστεί το ζεύγος (imdbID, year), το γράφουμε
                        context.write(key, value);                                                                      // Εγγραφή στο τελικό αποτέλεσμα
                        seenIdsAndYears.add(combinedKey);                                                               // Σημειώνουμε ότι το επεξεργαστήκαμε
                    }
                }
            }
        }
    }


    public static void main(String[] args) throws Exception {                                                           // Η μέθοδος main εκκινεί το Hadoop job

        if (args.length != 2) {                                                                                         // Ελέγχουμε ότι έχουν δοθεί σωστά τα μονοπάτια εισόδου και εξόδου
            System.err.println("Usage: DataCleaner <input_path> <output_path>");
            System.exit(-1);                                                                                            // Αν δεν υπάρχουν τα κατάλληλα μονοπάτια, βγαίνουμε με σφάλμα
        }

        Configuration conf = new Configuration();                                                                       // Δημιουργία ρυθμίσεων για το job
        Job job = Job.getInstance(conf, "IMDB Data Formatter");                                                         // Δημιουργία του job με όνομα
        job.setJarByClass(DataCleaner.class);                                                                           // Ορισμός της κλάσης που περιέχει το main

                                                                  // Ορισμός των Mapper και Reducer
        job.setMapperClass(IMDBMapper.class);
        job.setReducerClass(IMDBReducer.class);

                                                                  // Ορισμός των τύπων εξόδου
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

                                                                  // Ορισμός των μονοπατιών εισόδου και εξόδου
        FileInputFormat.addInputPath(job, new Path(args[0]));     // Μονοπάτι εισόδου
        FileOutputFormat.setOutputPath(job, new Path(args[1]));   // Μονοπάτι εξόδου

                                                                  // Εκτέλεση του job και επιστροφή αποτελέσματος
        System.exit(job.waitForCompletion(true) ? 0 : 1);         // Αν όλα πάνε καλά, επιστρέφει 0
    }
}
