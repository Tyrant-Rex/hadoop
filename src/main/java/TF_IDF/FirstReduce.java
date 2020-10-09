package TF_IDF;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FirstReduce extends Reducer<Text, IntWritable,Text,IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum=0;
        for (IntWritable i:values){
            sum+=i.get();
        }
        System.out.println(sum);

        if (key.equals(new Text("count"))){
            System.out.println(key.toString()+"________"+sum);
        }
        context.write(key,new IntWritable(sum));
    }
}
