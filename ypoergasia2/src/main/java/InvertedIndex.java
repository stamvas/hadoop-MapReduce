import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashSet;
import java.util.StringTokenizer;

public class InvertedIndex {

    public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text> {
        /**
         * παράμετρος Ν για το ελάχιστο μήκος λέξης
         * */
        private final int N = 10;

        private final Text word = new Text();
        private final Text fileNameText = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            // διαβάζει το όνομα του αρχείου
            String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
            // Αφαιρούμε όλους τους χαρακτήρες που δεν είναι γράμματα ή κενό
            // και μετατρέπουμε σε πεζά γράμματα
            StringTokenizer tokenizer = new StringTokenizer(value.toString().toLowerCase().replaceAll("[^a-zA-Z ]", ""));

            while (tokenizer.hasMoreTokens()) {
                String token = tokenizer.nextToken();

                // αν η λέξη είναι μεγαλύτερη απο Ν αποθηκεύουμε
                if (token.length() >= N) {
                    word.set(token);
                    fileNameText.set(fileName);
                    context.write(word, fileNameText);
                }
            }
        }
    }

    public static class InvertedIndexReducer extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {
            HashSet<String> uniqueFiles = new HashSet<>();
            for (Text val : values) {
                uniqueFiles.add(val.toString());
            }
            Text results = new Text(String.valueOf(uniqueFiles));
            context.write(key, results);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "inverted index");
        job.setJarByClass(InvertedIndex.class);
        job.setMapperClass(InvertedIndexMapper.class);
        job.setReducerClass(InvertedIndexReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
