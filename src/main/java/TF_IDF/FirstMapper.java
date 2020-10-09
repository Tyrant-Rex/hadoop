package TF_IDF;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.io.StringReader;

import java.io.IOException;
import java.io.StringReader;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.wltea.analyzer.lucene.*;
import org.wltea.analyzer.core.*;

public class FirstMapper extends Mapper<LongWritable, Text, Text, IntWritable>
{
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();

    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String line=value.toString().trim();
        System.out.println(line);
        StringReader sr = new StringReader(line);
        IKSegmenter ikSegmenter=new IKSegmenter(sr,true);
        Lexeme word = null;
        while ((word = ikSegmenter.next()) != null) {
            String w = word.getLexemeText();
            System.out.println(w);
            this.word.set(w);
            context.write(this.word,one);
        }
        context.write(new Text("count"),new IntWritable(1));

    }
}
