package TF_IDF;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class LastJob {
    public static void main(String[] args) {
        Configuration conf=new Configuration();
        conf.set("fs.defaultFS", "hdfs://10.0.0.2:9000");
        conf.set("mapreduce.app-submission.cross-platform", "true");

        try {
            FileSystem fs =FileSystem.get(conf);
            Job job =Job.getInstance(conf);
            job.setJarByClass(LastJob.class);
            job.setJobName("third");
//            job.setJar("./tfidf.jar");
            //把文档总数加载到
            job.addCacheFile(new Path("/output/first/part-r-00003").toUri());
            //把 df 加载到
            job.addCacheFile(new Path("/output/second/part-r-00000").toUri());
            //设置 map 任务的输出 key 类型、 value 类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            job.setMapperClass(LastMapper.class);
            job.setReducerClass(LastReducer.class);
            //mr 运行时的输入数据从 hdfs 的哪个目录中获取
            FileInputFormat.addInputPath(job, new Path("/output/first"));
            Path outpath =new Path("/output/third");
            if(fs.exists(outpath)){
                fs.delete(outpath, true);
            }
            FileOutputFormat.setOutputPath(job,outpath);
            boolean f= job.waitForCompletion(true);
            System.exit(f ? 0 : 1);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
