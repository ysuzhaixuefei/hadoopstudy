package com.study.hadoop;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * Created by zhaixuefei on 2018/2/23.
 */
public class WordCountPartition {
    public static class Map extends MapReduceBase  implements Mapper<LongWritable, Text, Text, IntWritable>{
        private static final IntWritable one = new IntWritable(1);
        private static Text word = new Text();
        @Override
        public void map(LongWritable longWritable, Text value, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);
            while(tokenizer.hasMoreTokens()){
                word.set(tokenizer.nextToken());
                outputCollector.collect(word,one);
            }
        }
    }
    public static class Reduce extends MapReduceBase implements Reducer<Text,IntWritable,Text,IntWritable>{

        @Override
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> outputCollector, Reporter reporter) throws IOException {
            int sum = 0;
            while(values.hasNext()){
                sum += values.next().get();
                outputCollector.collect(key,new IntWritable(sum));
            }
        }
    }
    public static void main(String [] args){
        JobConf job = new JobConf(WordCountPartition.class);
        job.setJobName("word.count.partition");

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setMapperClass(Map.class);
        job.setCombinerClass(Reduce.class);
        job.setReducerClass(Reduce.class);

        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
    }
}
