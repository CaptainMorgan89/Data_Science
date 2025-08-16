import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class MoviesPerYearGenre {

    // Mapper Class
    public static class YearGenreMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);                                                     // Δημιουργία σταθερού αντικειμένου με τιμή 1 για να μετράει τις ταινίες

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();                                                                      // Διαβάζουμε και αφαιρούμε τα κενά από τις αρχές και τα τέλη της γραμμής


            if (line.isEmpty() || line.startsWith("imdbID")) {                                                          // Αγνοούμε κενές γραμμές ή επικεφαλίδες
                return;                                                                                                 // Αν η γραμμή είναι κενή ή είναι επικεφαλίδα, παραλείπουμε αυτή τη γραμμή
            }

                                                                                                                        // Διαχωρισμός των πεδίων με κόμμα
            String[] fields = line.split(",");                                                                          // Διαχωρισμός της γραμμής σε πεδία με βάση το κόμμα


            if (fields.length >= 9) {                                                                                   // Βεβαιωνόμαστε ότι υπάρχουν τουλάχιστον 9 στήλες
                try {
                                                                                                                        // Παίρνουμε το έτος από τη στήλη 6 (index 5) που είναι σε μορφή yyyy-MM-dd
                    String date = fields[5].trim();                                                                     // Παίρνουμε την ημερομηνία από τη 6η στήλη (index 5)
                    String year = "";
                    if (!date.isEmpty() && date.matches("\\d{4}-\\d{2}-\\d{2}")) {                                      // Αν η ημερομηνία είναι έγκυρη
                        year = date.substring(0, 4);                                                                    // Παίρνουμε μόνο το έτος (τα πρώτα 4 ψηφία)
                    }

                                                                                                                        // Παίρνουμε το score (imdbRating) από τη στήλη 7 (index 6)
                    String ratingStr = fields[6].trim();                                                                // Παίρνουμε την τιμή της βαθμολογίας από τη 7η στήλη (index 6)
                    double rating = 0.0;
                    if (!ratingStr.isEmpty() && ratingStr.matches("\\d+(\\.\\d+)?")) {                                  // Αν η βαθμολογία είναι έγκυρη
                        rating = Double.parseDouble(ratingStr);                                                         // Μετατρέπουμε τη βαθμολογία σε αριθμό
                    }


                    String genres = fields[4].trim();                                                                   // Παίρνουμε τα είδη από τη στήλη 5 (index 4)

                                                                                                                        // Ελέγχουμε ότι έχουμε έγκυρη χρονιά, score > 8 και έγκυρα genres
                    if (!year.isEmpty() && rating > 8 && !genres.isEmpty()) {                                           // Αν έχουμε έγκυρη χρονιά, βαθμολογία > 8 και είδη
                                                                                                                        // Διαχωρίζουμε τα genres με βάση το "&"
                        String[] genreList = genres.split("&");                                                         // Διαχωρίζουμε τα genres με το "&"

                        for (String genre : genreList) {                                                                // Για κάθε genre στη λίστα
                            genre = genre.trim();                                                                       // Αφαιρούμε τα περιττά κενά
                            if (!genre.isEmpty()) {
                                                                                                                        // Δημιουργούμε το key με τη μορφή "year_genre"
                                String outputKey = year + "_" + genre;                                                  // Δημιουργούμε το κλειδί της εξόδου
                                context.write(new Text(outputKey), one);                                                // Γράφουμε το κλειδί και την τιμή (1)
                            }
                        }
                    }

                } catch (Exception e) {
                                                                                                                        // Αγνοούμε μη έγκυρες εγγραφές
                    System.err.println("Error processing line: " + line);                                               // Εκτύπωση σφάλματος αν προκύψει εξαίρεση
                }
            }
        }
    }

    // Reducer Class
    public static class YearGenreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {

            int totalMovies = 0;                                                                                        // Μετράμε τις ταινίες για κάθε "year_genre"

                                                                                                                        // Αθροίζουμε όλες τις ταινίες του συγκεκριμένου έτους-είδους
            for (IntWritable value : values) {                                                                          // Για κάθε τιμή που αντιστοιχεί σε αυτό το κλειδί
                totalMovies += value.get();                                                                             // Αθροίζουμε την τιμή (η οποία είναι πάντα 1)
            }

                                                                                                                        // Γράφουμε το τελικό αποτέλεσμα
            context.write(key, new IntWritable(totalMovies));                                                           // Γράφουμε το αποτέλεσμα στην έξοδο (κλειδί, σύνολο ταινιών)
        }
    }

    // Main Method
    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: MoviesPerYearGenre <input_path> <output_path>");                                 // Αν δε δοθούν τα κατάλληλα επιχειρήματα
            System.exit(-1);                                                                                            // Εκτυπώνουμε μήνυμα λάθους και τερματίζουμε το πρόγραμμα
        }

        Configuration conf = new Configuration();                                // Δημιουργία αντικειμένου Configuration για το Hadoop job
        Job job = Job.getInstance(conf, "Movies Per Year and Genre");            // Δημιουργία του Hadoop job με όνομα "Movies Per Year and Genre"

        job.setJarByClass(MoviesPerYearGenre.class);                             // Καθορισμός της κλάσης που περιέχει την κύρια μέθοδο του job
        job.setMapperClass(YearGenreMapper.class);                               // Καθορισμός της κλάσης Mapper
        job.setReducerClass(YearGenreReducer.class);                             // Καθορισμός της κλάσης Reducer

        job.setOutputKeyClass(Text.class);                                       // Καθορισμός του τύπου κλειδιού (key) για την έξοδο του job
        job.setOutputValueClass(IntWritable.class);                              // Καθορισμός του τύπου τιμής (value) για την έξοδο του job

        FileInputFormat.addInputPath(job, new Path(args[0]));                    // Καθορισμός του μονοπατιού εισόδου για τα δεδομένα του job
        FileOutputFormat.setOutputPath(job, new Path(args[1]));                  // Καθορισμός του μονοπατιού εξόδου για τα αποτελέσματα του job

        System.exit(job.waitForCompletion(true) ? 0 : 1);                        // Εκτέλεση του job και έξοδος με κωδικό 0 αν ολοκληρωθεί επιτυχώς, αλλιώς 1
    }
}
