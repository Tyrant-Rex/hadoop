package TF_IDF;

import com.sun.org.apache.regexp.internal.RE;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class FirstPartition extends HashPartitioner<Text, IntWritable> {
    @Override
    public int getPartition(Text key, IntWritable value, int numReduceTasks) {
        if (key.equals(new Text("count"))){
            return 3;
        }
        else {
            System.out.println("******************"+numReduceTasks);
            int result=super.getPartition(key, value, numReduceTasks-1);
            System.out.println("num of partitions:------------------"+result);
            return result;
        }
    }
}
