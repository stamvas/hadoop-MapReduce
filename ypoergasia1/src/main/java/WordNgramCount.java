import java.io.IOException;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordNgramCount {

    public static class NgramMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();


        /**
         * παράμετρος Ν για το μέγεθος των Ν-grams
         */
        private final int N = 4;




        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Μετατρέπουμε τη γραμμή εισόδου σε πεζά γράμματα
            String line = value.toString().toLowerCase();
            // Αφαιρούμε όλους τους χαρακτήρες που δεν είναι γράμματα ή κενό
            line = line.replaceAll("[^a-zA-Z ]", "");
            StringTokenizer tokenizer = new StringTokenizer(line);
            // Δημιουργούμε έναν πίνακα λέξεων
            String[] words = new String[tokenizer.countTokens()];
            int i = 0;
            // Ενώ υπάρχουν λέξεις στο tokenizer
            while (tokenizer.hasMoreTokens()) {
                // Αποθηκεύουμε κάθε λέξη στον πίνακα λέξεων
                words[i] = tokenizer.nextToken();
                i++;
            }

            // Δημιουργία n-grams
            for (i = 0; i < words.length - N + 1 ; i++) {
                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < N; j++) {
                    sb.append((j > 0 ? " " : "") + words[i + j]);
                }
                word.set(sb.toString());
                context.write(word, one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        /**
         * παράμετρος Κ για το πλήθος εμφανίσεων
         */
        private static final int K = 50;


        public void reduce(Text key, Iterable<IntWritable> values, Context context)
                throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            if (sum >= K) {
                result.set(sum);
                context.write(key, result);
            }
        }
    }


    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word n-gram count");
        job.setJarByClass(WordNgramCount.class);
        job.setMapperClass(NgramMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
