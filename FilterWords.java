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
public class FilterWords {
 public static class FilterMapper extends Mapper<Object, Text, Text,
IntWritable> {
 private IntWritable count = new IntWritable();
 public void map(Object key, Text value, Context context) throws
IOException, InterruptedException {
 String[] fields = value.toString().split("\t");
 String word = fields[0];
 int wordCount = Integer.parseInt(fields[1]);
 // Output only words with count greater than 2
 if (wordCount > 2) {
 count.set(wordCount);
 context.write(new Text(word), count);
 }
 }
 }
 public static void main(String[] args) throws Exception {
 Configuration conf = new Configuration();
 Job job = Job.getInstance(conf, "filter words");
 job.setJarByClass(FilterWords.class);
 job.setMapperClass(FilterMapper.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(IntWritable.class);
 FileInputFormat.addInputPath(job, new Path(args[0])); // Input
path from the first job's output
 FileOutputFormat.setOutputPath(job, new Path(args[1])); // Output
path
 System.exit(job.waitForCompletion(true) ? 0 : 1);
 }
}