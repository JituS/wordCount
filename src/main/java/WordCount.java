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
import java.util.HashMap;
import java.util.StringTokenizer;

public class WordCount {
    static class TotalMap extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer tokes = new StringTokenizer(value.toString());
            while(tokes.hasMoreTokens()) {
                word.set(tokes.nextToken());
                context.write(word, one);
            }
        }
    }

    static class TotalReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            context.write(key, new IntWritable(sum));
        }
    }

        static class TotalMap2 extends Mapper<Object, Text, Text, IntWritable> {
            private final static IntWritable one = new IntWritable(1);
            public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
                String[] tokens = value.toString().split("\\s+");
                for (int i = 0; i < tokens.length-1; i++) {
                    context.write(new Text(tokens[i]), new IntWritable(Integer.parseInt(tokens[i+1])));
                }
            }
        }

        static class TotalReduce2 extends Reducer<Text, IntWritable, IntWritable, IntWritable> {
            HashMap<Text, IntWritable> a = new HashMap<Text, IntWritable>();
            public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
                int sum = 0;
                for (IntWritable value : values) {
                    sum += value.get();
                }
                a.put(key, new IntWritable(sum));
                context.write(new IntWritable(a.size()), new IntWritable(a.size()));
            }
        }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        String tempPath = "/user/jitendrs/tempOut";
        Configuration conf1 = new Configuration();
        Job job = Job.getInstance(conf1, "Word Count");
        job.setJarByClass(WordCount.class);
        job.setCombinerClass(WordCount.TotalReduce.class);
        job.setReducerClass(WordCount.TotalReduce.class);
        job.setMapperClass(WordCount.TotalMap.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(tempPath));
        job.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Top 20");
        job2.setJarByClass(WordCount.class);
        job2.setReducerClass(WordCount.TotalReduce2.class);
        job2.setCombinerClass(WordCount.TotalReduce2.class);
        job2.setMapperClass(WordCount.TotalMap2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path(tempPath));
        FileOutputFormat.setOutputPath(job2, new Path(args[1]));
        System.exit(job2.waitForCompletion(true) ? 0 : 1);
    }
}


