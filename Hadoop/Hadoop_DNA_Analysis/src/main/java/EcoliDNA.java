import org.apache.hadoop.conf.Configuration;                                                                            // Παρέχει μηχανισμό ρύθμισης για τις εργασίες Hadoop
import org.apache.hadoop.fs.Path;                                                                                       // Χρησιμοποιείται για να διαχειρίζεται διαδρομές αρχείων στο HDFS
import org.apache.hadoop.io.IntWritable;                                                                                // Κλάση που αντιπροσωπεύει ακέραιες τιμές στο Hadoop
import org.apache.hadoop.io.Text;                                                                                       // Κλάση που αντιπροσωπεύει συμβολοσειρές στο Hadoop
import org.apache.hadoop.mapreduce.Job;                                                                                 // Αντιπροσωπεύει μια εργασία MapReduce
import org.apache.hadoop.mapreduce.Mapper;                                                                              // Βασική κλάση για τη φάση Map
import org.apache.hadoop.mapreduce.Reducer;                                                                             // Βασική κλάση για τη φάση Reduce
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;                                                           // Καθορίζει την είσοδο της εργασίας
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;                                                         // Καθορίζει την έξοδο της εργασίας
import java.io.IOException;                                                                                             // Χειρίζεται εξαιρέσεις εισόδου/εξόδου

public class EcoliDNA {

    public static class DNAMapper extends Mapper<Object, Text, Text, IntWritable> {                                     // Ο Mapper διαβάζει γραμμές DNA και εξάγει k-mers
        private final static IntWritable one = new IntWritable(1);                                                      // Σταθερή τιμή "1" για καταμέτρηση
        private Text kmer = new Text();                                                                                 // Μεταβλητή που αποθηκεύει τα k-mers

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString().trim();                                                                      // Μετατρέπει την είσοδο σε string και αφαιρεί τα κενά

            if (line.length() < 2) return;                                                                              // Αν η γραμμή έχει μήκος μικρότερο από 2, δεν κάνει τίποτα

            for (int i = 0; i < line.length(); i++) {                                                                   // Διατρέχει κάθε χαρακτήρα της γραμμής
                for (int k = 2; k <= 4; k++) {                                                                          // Δημιουργεί k-mers μήκους 2, 3 και 4
                    if (i + k <= line.length()) {                                                                       // Ελέγχει ότι το k-mer δεν υπερβαίνει το μήκος της γραμμής
                        kmer.set(line.substring(i, i + k));                                                             // Δημιουργεί το k-mer από την υποσυμβολοσειρά
                        context.write(kmer, one);                                                                       // Εκπέμπει το k-mer με count 1
                    }
                }
            }
        }
    }

    public static class DNAReducer extends Reducer<Text, IntWritable, Text, IntWritable> {                              // Ο Reducer μετρά τις εμφανίσεις κάθε k-mer
        private IntWritable result = new IntWritable();                                                                 // Μεταβλητή που αποθηκεύει το τελικό αποτέλεσμα

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;                                                                                                // Μετρητής για το άθροισμα των εμφανίσεων κάθε k-mer
            for (IntWritable val : values) {                                                                            // Διατρέχει όλες τις τιμές του ίδιου k-mer
                sum += val.get();                                                                                       // Προσθέτει τις εμφανίσεις
            }
            result.set(sum);                                                                                            // Θέτει το τελικό άθροισμα
            context.write(key, result);                                                                                 // Εκπέμπει το k-mer με τη συνολική του καταμέτρηση
        }
    }

    public static void main(String[] args) throws Exception {                                                           // Κύρια μέθοδος που διαχειρίζεται την εκτέλεση της εργασίας
        if (args.length != 2) {                                                                                         // Ελέγχει αν έχουν δοθεί τα σωστά ορίσματα εισόδου και εξόδου
            System.err.println("Usage: EcoliDNA <input path> <output path>");                                           // Εκτυπώνει μήνυμα λάθους
            System.exit(-1);                                                                                            // Τερματίζει το πρόγραμμα με κωδικό λάθους
        }

        Configuration conf = new Configuration();                                                                       // Δημιουργεί αντικείμενο ρυθμίσεων για την εργασία
        Job job = Job.getInstance(conf, "DNA k-mers Count");                                                            // Δημιουργεί την εργασία με όνομα "DNA k-mers Count"
        job.setJarByClass(EcoliDNA.class);                                                                              // Ορίζει την κύρια κλάση που περιέχει το πρόγραμμα
        job.setMapperClass(DNAMapper.class);                                                                            // Ορίζει την κλάση Mapper
        job.setReducerClass(DNAReducer.class);                                                                          // Ορίζει την κλάση Reducer

        job.setOutputKeyClass(Text.class);                                                                              // Ορίζει το κλειδί εξόδου ως Text (τα k-mers)
        job.setOutputValueClass(IntWritable.class);                                                                     // Ορίζει την τιμή εξόδου ως IntWritable (καταμέτρηση)

        FileInputFormat.addInputPath(job, new Path(args[0]));                                                           // Ορίζει τη διαδρομή εισόδου (δεδομένα DNA)
        FileOutputFormat.setOutputPath(job, new Path(args[1]));                                                         // Ορίζει τη διαδρομή εξόδου (αποτελέσματα)

        System.exit(job.waitForCompletion(true) ? 0 : 1);                                                               // Εκτελεί την εργασία και επιστρέφει τον αντίστοιχο κωδικό επιτυχίας
    }
}
