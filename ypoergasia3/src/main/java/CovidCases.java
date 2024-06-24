import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class CovidCases {

    // Mapper για τον υπολογισμό μηνιαίου αριθμού κρουσμάτων
    public static class MonthlyCasesMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final Text month = new Text();
        private final IntWritable cases = new IntWritable();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Διαχωρίζει τις γραμμές δεδομένων και παίρνει τον μήνα και τον αριθμό κρουσμάτων
            String[] parts = value.toString().split(",");
            if (parts.length >= 12) {
                String dateString = parts[0].trim(); // Ημερομηνία
                String[] reverseDate = dateString.split("/");
                if (reverseDate.length == 3) {
                    String monthString = reverseDate[1].trim(); // Μήνας
                    month.set(monthString);
                    int dailyCases = Integer.parseInt(parts[4].trim()); // Ημερήσια κρούσματα
                    cases.set(dailyCases);
                    // αποθηκεύει τον μήνα ως κλειδί και τον αριθμό των κρουσμάτων ως τιμή
                    context.write(month, cases);
                }
            }
        }
    }

    // Reducer για τον υπολογισμό μέσου όρου κρουσμάτων ανά μήνα
    public static class MonthlyCasesReducer extends Reducer<Text, IntWritable, Text, DoubleWritable> {

        private final Map<String, Double> monthToMean = new HashMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            int count = 0;
            for (IntWritable val : values) {
                sum += val.get();
                count++;
            }
            // Υπολογίζει το μέσο όρο
            double average = (double) sum / count;
            // Αποθηκεύει το μέσο όρο στο Map
            monthToMean.put(key.toString(), average);
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Επιστρέφει το μέσο όρο για κάθε μήνα
            for (Map.Entry<String, Double> entry : monthToMean.entrySet()) {
                context.write(new Text(entry.getKey()), new DoubleWritable(entry.getValue()));
            }
        }
    }

    // Mapper για τον εντοπισμό ημερών με κρούσματα άνω του μέσου όρου
    public static class AboveMeanCasesMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final Text month = new Text();
        private final IntWritable dailyCase = new IntWritable();
        private final Map<String, Double> monthToMean = new HashMap<>();

        protected void setup(Context context) throws IOException, InterruptedException {
            // Διαβάζει το αρχείο και αποθηκεύει τον μέσο όρο για κάθε μήνα
            Configuration conf = context.getConfiguration();
            Path meanFilePath = new Path(conf.get("meanFilePath"));
            FileSystem fs = null;
            try {
                fs = FileSystem.get(new URI(meanFilePath.toString()), conf);
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
            try (BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(meanFilePath)))) {
                String line;
                while ((line = br.readLine()) != null) {
                    String[] parts = line.split("\t");
                    if (parts.length == 2) {
                        monthToMean.put(parts[0], Double.parseDouble(parts[1]));
                    }
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] parts = value.toString().split(",");
            if (parts.length >= 11) {
                String dateString = parts[0].trim(); // Ημερομηνία
                String[] reverseDate = dateString.split("/");
                if (reverseDate.length == 3) {
                    String monthString = reverseDate[1].trim(); // Μήνας
                    String country = parts[6].trim(); // Χώρα
                    String year = parts[3].trim(); // έτος
                    // Μέσος όρος ανά μήνα
                    double mean = monthToMean.getOrDefault(monthString, 0.0); // Χρησιμοποιούμε τον μέσο όρο ανά μήνα
                    // Ημερήσια κρούσματα
                    int dailyCases = Integer.parseInt(parts[4].trim()); // Daily cases
                    if (dailyCases > mean) {
                        month.set(year + "-" + monthString + "-" + country);
                        dailyCase.set(dailyCases);
                        // Αποθηκεύει το κλειδί (έτος-μήνας-χώρα) και τα ημερήσια κρούσματα
                        context.write(month, dailyCase);
                    }
                }
            }
        }
    }

    // Reducer για τον υπολογισμό του αριθμού των ημερών με κρούσματα άνω του μέσου όρου
    public static class AboveMeanCasesReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable val : values) {
                count++;
            }
            // Αποθηκεύει τον αριθμό των ημερών με κρούσματα άνω του μέσου όρου
            context.write(key, new IntWritable(count));
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job1 = Job.getInstance(conf, "Monthly Average Cases");
        job1.setJarByClass(CovidCases.class);
        job1.setMapperClass(MonthlyCasesMapper.class);
        job1.setReducerClass(MonthlyCasesReducer.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job1, new Path("input"));
        FileOutputFormat.setOutputPath(job1, new Path("temp"));

        if (job1.waitForCompletion(true)) {
            Configuration conf2 = new Configuration();
            conf2.set("meanFilePath", "temp/part-r-00000");
            Job job2 = Job.getInstance(conf2, "Cases Above Mean");
            job2.setJarByClass(CovidCases.class);
            job2.setMapperClass(AboveMeanCasesMapper.class);
            job2.setReducerClass(AboveMeanCasesReducer.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(IntWritable.class);
            MultipleInputs.addInputPath(job2, new Path("input"), TextInputFormat.class, AboveMeanCasesMapper.class); // Input path for the dataset
            FileOutputFormat.setOutputPath(job2, new Path("output"));
            System.exit(job2.waitForCompletion(true) ? 0 : 1);
        } else {
            System.exit(1);
        }
    }
}
