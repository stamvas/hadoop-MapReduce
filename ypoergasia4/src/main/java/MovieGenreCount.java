import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MovieGenreCount {

    public static class MovieGenreMapper extends Mapper<Object, Text, Text, IntWritable> {

        private final static IntWritable one = new IntWritable(1);
        private final Text word = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // Διαχωρισμός της γραμμής εισόδου με βάση το "::"
            String[] fields = value.toString().split("::");
            // Έλεγχος εάν η γραμμή έχει τουλάχιστον 3 πεδία
            if (fields.length >= 3) {
                // Διαχωρισμός των ειδών με βάση το "|"
                String[] genres = fields[2].split("\\|");
                // διπλό loop για το είδος της ταινίας
                for (String genre1 : genres) {
                    for (String genre2 : genres) {
                        // αν είναι διαφορετικά τα αποθηκεύει σαν κλειδί
                        if (!genre1.equals(genre2)) {
                            word.set(genre1 + "_" + genre2);
                            context.write(word, one);
                        }
                    }
                }
            }
        }
    }

    public static class MovieGenreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        private final IntWritable result = new IntWritable();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "movie genre count");
        job.setJarByClass(MovieGenreCount.class);
        job.setMapperClass(MovieGenreMapper.class);
        job.setCombinerClass(MovieGenreReducer.class);
        job.setReducerClass(MovieGenreReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path("input"));
        FileOutputFormat.setOutputPath(job, new Path("output"));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
