import org.apache.hadoop.conf.Configuration;                   // Εισαγωγή της κλάσης Configuration από το Hadoop
import org.apache.hadoop.fs.Path;                              // Εισαγωγή της κλάσης Path για να καθορίσουμε τα paths εισόδου/εξόδου
import org.apache.hadoop.io.LongWritable;                      // Εισαγωγή της κλάσης LongWritable για τη διαχείριση ακέραιων τιμών
import org.apache.hadoop.io.Text;                              // Εισαγωγή της κλάσης Text για τη διαχείριση κειμένων
import org.apache.hadoop.mapreduce.Job;                        // Εισαγωγή της κλάσης Job για την εκτέλεση του MapReduce job
import org.apache.hadoop.mapreduce.Mapper;                     // Εισαγωγή της κλάσης Mapper
import org.apache.hadoop.mapreduce.Reducer;                    // Εισαγωγή της κλάσης Reducer
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;  // Εισαγωγή της κλάσης FileInputFormat για το input του Hadoop job
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;// Εισαγωγή της κλάσης FileOutputFormat για το output του Hadoop job
import java.io.IOException;                                    // Εισαγωγή για χειρισμό εξαιρέσεων

public class CountryMovieDuration {                           // Κλάση που υπολογίζει τη συνολική διάρκεια των ταινιών ανά χώρα

                                                                                                                        // Mapper Class: Διαβάζει τα δεδομένα και στέλνει τη χώρα και τη διάρκεια στην έξοδο
    public static class CountryDurationMapper extends Mapper<Object, Text, Text, LongWritable> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();                                                                      // Μετατρέπει την τιμή σε String και αφαιρεί τυχόν περιττά κενά

                                                                                                                        // Αγνοούμε κενές γραμμές ή επικεφαλίδες
            if (line.isEmpty() || line.startsWith("imdbID")) {                                                          // Αν η γραμμή είναι κενή ή αρχίζει με την επικεφαλίδα "imdbID"
                return;                                                                                                 // Αγνοούμε αυτή τη γραμμή
            }

                                                                                                                        // Διαχωρίζουμε τη γραμμή με κόμμα (txt format)
            String[] fields = line.split(",");                                                                          // Χωρίζουμε τη γραμμή σε στήλες με το κόμμα

                                                                                                                        // Ελέγχουμε αν υπάρχουν τουλάχιστον 9 στήλες (σύμφωνα με τα δεδομένα σου)
            if (fields.length >= 9) {                                                                                   // Εάν η γραμμή έχει τουλάχιστον 9 στήλες
                try {
                                                                                                                        // Παίρνουμε τη διάρκεια από τη στήλη 3 (index 3)
                    String durationStr = fields[3].trim();                                                              // Παίρνουμε την τιμή της στήλης 3 που είναι η διάρκεια

                                                                                                                        // Αν η διάρκεια είναι κενή ή δεν έχει την κατάλληλη μορφή, την αγνοούμε
                    if (durationStr.isEmpty() || !durationStr.matches("\\d+ min")) {                                    // Αν η διάρκεια δεν είναι έγκυρη
                        return;                                                                                         // Αγνοούμε αυτή τη γραμμή
                    }

                                                                                                                        // Εξάγουμε τη διάρκεια σε minutes
                    long duration = Long.parseLong(durationStr.replace(" min", ""));                                    // Αφαιρούμε το "min" και μετατρέπουμε σε αριθμό

                                                                                                                        // Παίρνουμε τη χώρα από τη στήλη 8 (index 7)
                    String countries = fields[8].trim();                                                                // Παίρνουμε την τιμή της στήλης 8 που είναι η χώρα

                                                                                                                        // Αν η στήλη της χώρας είναι κενή, την αγνοούμε
                    if (countries.isEmpty()) {                                                                          // Αν η χώρα είναι κενή
                        return;                                                                                         // Αγνοούμε τη γραμμή
                    }

                                                                                                                        // Χωρίζουμε τις χώρες με βάση το "&"
                    String[] countryList = countries.split("&");                                                        // Χωρίζουμε τις χώρες αν είναι πολλαπλές

                    for (String country : countryList) {                                                                // Επαναλαμβάνουμε για κάθε χώρα
                        country = country.trim();                                                                       // Καθαρίζουμε τυχόν κενά γύρω από το όνομα της χώρας
                        if (!country.isEmpty()) {                                                                       // Αν η χώρα δεν είναι κενή
                                                                                                                        // Γράφουμε την κάθε χώρα και τη διάρκεια στο context
                            context.write(new Text(country), new LongWritable(duration));                               // Στέλνουμε τη χώρα και τη διάρκεια στον reducer
                        }
                    }
                } catch (Exception e) {
                                                                                                                        // Αγνοούμε μη έγκυρες εγγραφές
                    System.err.println("Error processing line: " + line);                                               // Εκτυπώνουμε σφάλμα αν υπάρχει κάποιο πρόβλημα με τη γραμμή
                }
            }
        }
    }

                                                                                                                        // Reducer Class: Αθροίζει τις διάρκειες για κάθε χώρα
    public static class CountryDurationReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

        @Override
        public void reduce(Text key, Iterable<LongWritable> values, Context context)
                throws IOException, InterruptedException {

            long totalDuration = 0;                                                                                     // Αρχικοποιούμε τη συνολική διάρκεια σε 0

                                                                                                                        // Αθροίζουμε όλες τις διάρκειες για την συγκεκριμένη χώρα
            for (LongWritable value : values) {                                                                         // Για κάθε διάρκεια που μας στέλνει ο Mapper
                totalDuration += value.get();                                                                           // Προσθέτουμε τη διάρκεια
            }
                                                                                                                        // Γράφουμε τη χώρα και τη συνολική διάρκεια
            context.write(key, new LongWritable(totalDuration));                                                        // Στέλνουμε τη χώρα και τη συνολική διάρκεια στον output
        }
    }


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {                                                                                         // Αν δεν έχουμε ακριβώς δύο παραμέτρους από τη γραμμή εντολών
            System.err.println("Usage: CountryMovieDuration <input_path> <output_path>");                               // Εμφανίζουμε μήνυμα χρήσης
            System.exit(-1);                                                                                            // Τερματίζουμε την εκτέλεση με κωδικό σφάλματος
        }


        Configuration conf = new Configuration();                                                                       // Δημιουργούμε μια νέα ρύθμιση για το job
        Job job = Job.getInstance(conf, "Country Movie Duration");                                                      // Δημιουργούμε το job με όνομα "Country Movie Duration"

        job.setJarByClass(CountryMovieDuration.class);                                                                  // Ορίζουμε το jar που περιέχει την κλάση του job


        job.setMapperClass(CountryDurationMapper.class);                                                                // Ορίζουμε τη Mapper κλάση
        job.setReducerClass(CountryDurationReducer.class);                                                              // Ορίζουμε τη Reducer κλάση


        job.setOutputKeyClass(Text.class);                                                                              // Ορίζουμε το τύπο του κλειδιού εξόδου
        job.setOutputValueClass(LongWritable.class);                                                                    // Ορίζουμε το τύπο της τιμής εξόδου


        FileInputFormat.addInputPath(job, new Path(args[0]));                                                           // Ορίζουμε το input path
        FileOutputFormat.setOutputPath(job, new Path(args[1]));                                                         // Ορίζουμε το output path


        System.exit(job.waitForCompletion(true) ? 0 : 1);                                                               // Εκτελούμε το job και εξέρχεται με κωδικό 0 αν ολοκληρωθεί σωστά
    }
}
