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

public class NumeronymsCount {

    public static class NumeronymMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);                      // Σταθερή τιμή 1 για κάθε εμφάνιση
        private final Text numeronym = new Text();                                      // Κείμενο εξόδου του mapper

        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {
            String line = value.toString().toLowerCase();                         // Μετατροπή σε πεζά (case-insensitive)
            line = line.replaceAll("[^a-zA-Z]", " ");                             // Αφαίρεση σημείων στίξης
            String[] words = line.split("\\s+");                                  // Διαχωρισμός λέξεων με κενά

            for (String word : words) {
                if (word.length() < 3) continue;                                  // Παράλειψη λέξεων με λιγότερους από 3 χαρακτήρες

                String num = word.charAt(0) +                                     // Πρώτος χαρακτήρας +
                        String.valueOf(word.length() - 2) +                       // πλήθος χαρακτήρων ενδιάμεσα +
                        word.charAt(word.length() - 1);                           // τελευταίος χαρακτήρας
                numeronym.set(num);                                               // Ορισμός του numeronym
                context.write(numeronym, one);                                    // Εκπομπή (numeronym, 1)
            }
        }
    }

    public static class NumeronymReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private int minCount;                                                    // Ελάχιστο πλήθος εμφανίσεων

        @Override
        protected void setup(Context context) {
            Configuration conf = context.getConfiguration();
            minCount = conf.getInt("min.count", 1);                              // Ανάγνωση παραμέτρου min.count (προεπιλογή: 1)
        }

        @Override
        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();                                                // Άθροιση όλων των τιμών
            }

            if (sum >= minCount) {                                               // Εκπομπή μόνο αν το πλήθος >= minCount
                context.write(key, new IntWritable(sum));
            }
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: NumeronymsCount <input path> <output path> <min count>");                        // Έξοδος αν δεν δοθούν 3 παράμετροι
            System.exit(-1);
        }

        Configuration conf = new Configuration();
        conf.setInt("min.count", Integer.parseInt(args[2]));                     // Ρύθμιση παραμέτρου min.count

        Job job = Job.getInstance(conf, "Numeronym Count");                      // Δημιουργία και ορισμός job
        job.setJarByClass(NumeronymsCount.class);

        job.setMapperClass(NumeronymMapper.class);                               // Ορισμός Mapper
        job.setReducerClass(NumeronymReducer.class);                             // Ορισμός Reducer

        job.setOutputKeyClass(Text.class);                                       // Τύπος κλειδιού εξόδου
        job.setOutputValueClass(IntWritable.class);                              // Τύπος τιμής εξόδου

        FileInputFormat.addInputPath(job, new Path(args[0]));                    // Διαδρομή εισόδου
        FileOutputFormat.setOutputPath(job, new Path(args[1]));                  // Διαδρομή εξόδου

        System.exit(job.waitForCompletion(true) ? 0 : 1);                        // Εκτέλεση job και επιστροφή αποτελέσματος
    }
}
