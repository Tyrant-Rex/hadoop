package TF_IDF;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SecondJob {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.0.0.2:9000");

        conf.set("mapreduce.app-submission.coress-paltform", "true");
        conf.set("mapreduce.framework.name", "local");

        try {
            FileSystem fs=FileSystem.get(conf);
            Job job =Job.getInstance(conf);
            job.setJarByClass(SecondJob.class);
            job.setJobName("second");
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            job.setMapperClass(SecondMapper.class);
            job.setCombinerClass(SecondReducer.class);
            job.setReducerClass(SecondReducer.class);
            FileInputFormat.addInputPath(job, new Path("/output/first"));

            Path path2=new Path("/output/second");
            if (fs.exists(path2)){
                fs.delete(path2,true);
            }

            FileOutputFormat.setOutputPath(job, path2);
            boolean f= job.waitForCompletion(true);
            if(f){
                System.out.println("执行 job 成功");
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
