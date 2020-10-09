package TF_IDF;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.Arrays;

public class SecondMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FileSplit fs = (FileSplit) context.getInputSplit();
//        System.out.println(fs.getPath().getName());
        if (!fs.getPath().getName().contains("part-r-00003")) {
            String[] v = value.toString().trim().split("\t");
            System.out.println(Arrays.toString(v));
            if (v.length >= 2) {
                String[] ss = v[0].split("_");
                if (ss.length >= 2) {
                    String w = ss[0];
                    context.write(new Text(w), new IntWritable(1));
                }
            } else {
                System.out.println(value.toString() + "-------------");
            }
        }
    }
}
