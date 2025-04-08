
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;

import org.apache.hadoop.mapreduce.Reducer;

public class testreduce extends Reducer<IntWritable, IntWritable,
IntWritable, IntWritable> {
@Override
        public void reduce(IntWritable key, Iterable<IntWritable> values,
Context context)  throws IOException, InterruptedException {

        for (IntWritable value : values) {

            context.write(value,key);

           }

}
}



//Step 1: Export Java Eclipse Project Jar File to Cloudera
//Step 2. Make Sort.txt file vi editor ->Write data
//Step 3: Perform Below commands on terminal
//
//Command Map Reduce Code
//
//1)	Transfer all local file to hadoop 
//
//Hdfs dfs –put sort.txt /user/cloudera
//
//Hdfs dfs –put Sorting.jar /user/cloudera
//
//2)	Run Java Jar File for Map Reduce Operation
//
//hadoop jar Sorting.jar testdriver sort.txt outputsort
//
//3)	List outputfile
//
//hdfs dfs –ls /user/cloudera/outputsort
//
//4)	Show outputfile
//
//hdfs dfs –cat /user/cloudera/outputsort /part-r-00000
//

