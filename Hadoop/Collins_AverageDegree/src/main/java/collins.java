import org.apache.hadoop.conf.Configuration;                                                                            // Εισάγεται η βιβλιοθήκη για τη διαχείριση της διαμόρφωσης του Hadoop.
import org.apache.hadoop.fs.Path;                                                                                       // Εισάγεται η βιβλιοθήκη για τη διαχείριση των μονοπατιών των αρχείων.
import org.apache.hadoop.io.DoubleWritable;                                                                             // Εισάγεται η βιβλιοθήκη για την αποθήκευση αριθμών κινητής υποδιαστολής (double).
import org.apache.hadoop.io.IntWritable;                                                                                // Εισάγεται η βιβλιοθήκη για την αποθήκευση ακέραιων αριθμών.
import org.apache.hadoop.io.Text;                                                                                       // Εισάγεται η βιβλιοθήκη για την αποθήκευση κειμένων (String).
import org.apache.hadoop.mapreduce.Job;                                                                                 // Εισάγεται η βιβλιοθήκη για τη δημιουργία και διαχείριση του MapReduce job.
import org.apache.hadoop.mapreduce.Mapper;                                                                              // Εισάγεται η βιβλιοθήκη για την εφαρμογή του Mapper.
import org.apache.hadoop.mapreduce.Reducer;                                                                             // Εισάγεται η βιβλιοθήκη για την εφαρμογή του Reducer.
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;                                                           // Εισάγεται η βιβλιοθήκη για τη διαχείριση του input.
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;                                                         // Εισάγεται η βιβλιοθήκη για τη διαχείριση του output.
import java.io.IOException;                                                                                             // Εισάγεται η βιβλιοθήκη για τη διαχείριση εξαιρέσεων.

public class collins {

    public static class DegreeMapper extends Mapper<Object, Text, IntWritable, DoubleWritable> {                        // Ο Mapper αναλαμβάνει την εξαγωγή των δεδομένων από τις γραμμές του input αρχείου.
        private double threshold;                                                                                       // Ορίζεται μία μεταβλητή για το κατώφλι (threshold) που θα καθορίσει αν η σχέση κόμβου-κόμβου είναι έγκυρη.
        private final IntWritable node = new IntWritable();                                                             // Δημιουργούμε το αντικείμενο για να κρατάμε τον κόμβο (node).
        private final DoubleWritable weight = new DoubleWritable();                                                     // Δημιουργούμε το αντικείμενο για να κρατάμε το βάρος (weight).

        @Override
        protected void setup(Context context) {                                                                         // Η μέθοδος setup καλείται πριν την επεξεργασία του input.
            Configuration conf = context.getConfiguration();                                                            // Λαμβάνουμε τη διαμόρφωση του context για να αποκτήσουμε το κατώφλι (threshold).
            threshold = Double.parseDouble(conf.get("threshold"));                                                      // Ανάγνωση του threshold από τη διαμόρφωση.
        }

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {             // Ο Mapper επεξεργάζεται κάθε γραμμή από το input.
            String[] parts = value.toString().split(" ");                                                               // Διαχωρίζουμε τη γραμμή στα 3 μέρη: κόμβος 1, κόμβος 2 και βάρος (probability).
            if (parts.length != 3) return;                                                                              // Εάν δεν υπάρχουν 3 στοιχεία στη γραμμή, παραλείπουμε αυτήν τη γραμμή.

            int node1 = Integer.parseInt(parts[0]);                                                                     // Ο πρώτος κόμβος.
            int node2 = Integer.parseInt(parts[1]);                                                                     // Ο δεύτερος κόμβος.
            double prob = Double.parseDouble(parts[2]);                                                                 // Το βάρος της σύνδεσης μεταξύ των δύο κόμβων.

            if (prob >= threshold) {                                                                                    // Αν το βάρος είναι μεγαλύτερο ή ίσο από το κατώφλι (threshold), τότε το καταγράφουμε.
                node.set(node1);                                                                                        // Ορίζουμε τον πρώτο κόμβο.
                weight.set(prob);                                                                                       // Ορίζουμε το βάρος.
                context.write(node, weight);                                                                            // Γράφουμε το αποτέλεσμα για τον πρώτο κόμβο.

                node.set(node2);                                                                                        // Ορίζουμε τον δεύτερο κόμβο.
                context.write(node, weight);                                                                            // Γράφουμε το αποτέλεσμα για τον δεύτερο κόμβο.
            }
        }
    }


    public static class DegreeReducer extends Reducer<IntWritable, DoubleWritable, IntWritable, DoubleWritable> {       // Ο Reducer παίρνει τα δεδομένα από τον Mapper και τα συγκεντρώνει.

        private final DoubleWritable sumWeight = new DoubleWritable();                                                  // Δημιουργούμε το αντικείμενο για να αποθηκεύσουμε το άθροισμα των βαρών.

        @Override
        public void reduce(IntWritable key, Iterable<DoubleWritable> values, Context context)                           // Ο Reducer επεξεργάζεται τα δεδομένα που παρέχονται από τον Mapper.
                throws IOException, InterruptedException {

            double sum = 0.0;                                                                                           // Αρχικοποιούμε το άθροισμα των βαρών.

            for (DoubleWritable val : values) {                                                                         // Για κάθε βάρος που έρχεται από τον Mapper.
                sum += val.get();                                                                                       // Προσθέτουμε το βάρος στο άθροισμα.
            }

            sumWeight.set(sum);                                                                                         // Ορίζουμε το συνολικό άθροισμα του βάρους.
            context.write(key, sumWeight);                                                                              // Γράφουμε το τελικό αποτέλεσμα για τον κόμβο.
        }
    }


    public static void main(String[] args)                                                                              // Η μέθοδος main για την εκτέλεση του job.
            throws Exception {

        if (args.length != 3) {                                                                                         // Αν δεν υπάρχουν 3 ορίσματα (input path, output path, threshold), εμφανίζουμε μήνυμα σφάλματος.
            System.err.println("Usage: AverageDegree <input path> <output path> <threshold>");
            System.exit(-1);                                                                                            // Κλείνουμε το πρόγραμμα αν τα ορίσματα είναι λανθασμένα.
        }

        Configuration conf = new Configuration();                                                                       // Δημιουργούμε τη διαμόρφωση του job.
        conf.set("threshold", args[2]);                                                                                 // Ρυθμίζουμε το threshold που περνάει ως όρισμα από τη γραμμή εντολών.

        Job job = Job.getInstance(conf, "Average Degree Calculation");                                                  // Δημιουργούμε το Job με την αντίστοιχη διαμόρφωση και όνομα.
        job.setJarByClass(collins.class);                                                                               // Ορίζουμε την κλάση του jar που θα εκτελεστεί.
        job.setMapperClass(DegreeMapper.class);                                                                         // Ορίζουμε τον Mapper για την εκτέλεση.
        job.setReducerClass(DegreeReducer.class);                                                                       // Ορίζουμε τον Reducer για την εκτέλεση.

        job.setOutputKeyClass(IntWritable.class);                                                                       // Ορίζουμε τον τύπο του κλειδιού εξόδου.
        job.setOutputValueClass(DoubleWritable.class);                                                                  // Ορίζουμε τον τύπο της τιμής εξόδου.

        FileInputFormat.addInputPath(job, new Path(args[0]));                                                           // Ορίζουμε την είσοδο του αρχείου.
        FileOutputFormat.setOutputPath(job, new Path(args[1]));                                                         // Ορίζουμε την έξοδο του αρχείου.

        System.exit(job.waitForCompletion(true) ? 0 : 1);                                                               // Εκκινείται το job και επιστρέφεται το κατάλληλο αποτέλεσμα.
    }
}