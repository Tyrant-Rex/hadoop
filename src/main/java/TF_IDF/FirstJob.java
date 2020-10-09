package TF_IDF;

import com.google.inject.internal.cglib.core.$Signature;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.FileOutputStream;
import java.io.IOException;

public class FirstJob {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        // add the network address
        conf.set("fs.defaultFS", "hdfs://10.0.0.2:9000");

        conf.set("mapreduce.app-submission.coress-platform", "true");
        conf.set("mapreduce.framework.name", "local");

        try {
            FileSystem fs=FileSystem.get(conf);

            Job job=Job.getInstance(conf);
            job.setJarByClass(FirstJob.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setNumReduceTasks(4);
            job.setPartitionerClass(FirstPartition.class);
            job.setMapperClass(FirstMapper.class);
            // Map 阶段有一个分割成组的操作
            job.setCombinerClass(FirstReduce.class);
            job.setReducerClass(FirstReduce.class);

            Path outputpath=new Path("/output/first");
            if (fs.exists(outputpath)){
                fs.delete(outputpath,true);
            }

            FileInputFormat.addInputPath(job,new Path("/input2"));
            FileOutputFormat.setOutputPath(job,outputpath);

            boolean f=job.waitForCompletion(true);
            System.exit(f?0:1);

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
}
