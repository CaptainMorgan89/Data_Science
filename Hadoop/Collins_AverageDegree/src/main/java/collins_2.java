import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class collins_2 {

    public static class DegreeMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {                        // Η κλάση DegreeMapper επεξεργάζεται κάθε γραμμή από το input αρχείο.
        private final IntWritable node = new IntWritable();                                                             // Δημιουργούμε ένα αντικείμενο IntWritable για τον κόμβο (node).
        private final DoubleWritable weight = new DoubleWritable();                                                     // Δημιουργούμε ένα αντικείμενο DoubleWritable για το βάρος (degree).

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {

            String[] parts = value.toString().split("\\s+");                                                            // Διαχωρίζουμε τη γραμμή σε δύο μέρη, τον κόμβο και τον βαθμό (degree).
            if (parts.length != 2) return;                                                                              // Ελέγχουμε αν υπάρχουν ακριβώς 2 στοιχεία στη γραμμή.

            int nodeId = Integer.parseInt(parts[0]);                                                                    // Ο πρώτος αριθμός είναι ο κόμβος (node).
            double degree = Double.parseDouble(parts[1]);                                                               // Ο δεύτερος αριθμός είναι ο βαθμός (degree).

            node.set(nodeId);                                                                                           // Ορίζουμε το κόμβο.
            weight.set(degree);                                                                                         // Ορίζουμε το βάρος.
            context.write(node, weight);                                                                                // Γράφουμε το αποτέλεσμα (κόμβο, βάρος) για τον Mapper.
        }
    }

    public static class DegreeReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {

        private final List<IntWritable> nodes = new ArrayList<>();                                                      // Λίστα για να αποθηκεύσουμε τους κόμβους.
        private final List<DoubleWritable> degrees = new ArrayList<>();                                                 // Λίστα για να αποθηκεύσουμε τα βάρη (βαθμούς).
        private double sum = 0.0;                                                                                       // Μεταβλητή για το άθροισμα όλων των βαθμών.
        private int count = 0;                                                                                          // Μετρητής για τον αριθμό των κόμβων.

        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)  {                        // Ο Reducer επεξεργάζεται τα δεδομένα για κάθε κόμβο.
            double totalDegree = 0.0;                                                                                   // Αρχικοποιούμε το συνολικό βάρος για τον συγκεκριμένο κόμβο.

            for (DoubleWritable val : values) {                                                                         // Για κάθε βάρος που επιστρέφεται από τον Mapper για αυτόν τον κόμβο.
                totalDegree += val.get();                                                                               // Προσθέτουμε το βάρος στο συνολικό βάρος.
            }

            nodes.add(new IntWritable(key.get()));                                                                      // Προσθέτουμε τον κόμβο στη λίστα κόμβων.
            degrees.add(new DoubleWritable(totalDegree));                                                               // Προσθέτουμε τον συνολικό βαθμό στη λίστα των βαθμών.
            sum += totalDegree;                                                                                         // Προσθέτουμε τον συνολικό βαθμό στο συνολικό άθροισμα όλων των βαθμών.
            count++;                                                                                                    // Αυξάνουμε τον μετρητή για τον αριθμό των κόμβων.
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {                              // Η μέθοδος cleanup εκτελείται αφού ολοκληρωθεί το reduce.
            double averageDegree = sum / count;                                                                         // Υπολογίζουμε τον μέσο όρο των βαθμών.

            for (int i = 0; i < nodes.size(); i++) {                                                                    // Για κάθε κόμβο στη λίστα.
                if (degrees.get(i).get() > averageDegree) {                                                             // Αν ο βαθμός του κόμβου είναι μεγαλύτερος από τον μέσο όρο.
                    context.write(nodes.get(i), degrees.get(i));                                                        // Γράφουμε τον κόμβο και τον βαθμό του ως αποτέλεσμα.
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {                                                           // Η μέθοδος main εκτελεί το job του MapReduce.
        if (args.length != 2) {                                                                                         // Αν δεν υπάρχουν δύο ορίσματα (input path και output path).
            System.err.println("Usage: Collins <input path> <output path>");                                            // Εμφανίζουμε μήνυμα χρήσης.
            System.exit(-1);                                                                                            // Τερματίζουμε το πρόγραμμα αν τα ορίσματα είναι λανθασμένα.
        }

        Configuration conf = new Configuration();                                                                       // Δημιουργούμε τη διαμόρφωση του job.
        Job job = Job.getInstance(conf, "Filter High Degree Nodes");                                                    // Δημιουργούμε το job με όνομα "Filter High Degree Nodes".
        job.setJarByClass(collins_2.class);                                // Ορίζουμε την κλάση για το jar.
        job.setMapperClass(DegreeMapper.class);                            // Ορίζουμε την κλάση Mapper.
        job.setReducerClass(DegreeReducer.class);                          // Ορίζουμε την κλάση Reducer.

        job.setOutputKeyClass(IntWritable.class);                          // Ορίζουμε τον τύπο του κλειδιού εξόδου.
        job.setOutputValueClass(DoubleWritable.class);                     // Ορίζουμε τον τύπο της τιμής εξόδου.

        FileInputFormat.addInputPath(job, new Path(args[0]));              // Ορίζουμε το αρχείο εισόδου.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));            // Ορίζουμε το αρχείο εξόδου.

        System.exit(job.waitForCompletion(true) ? 0 : 1);                  // Εκκινεί το job και επιστρέφει το κατάλληλο αποτέλεσμα.
    }
}
