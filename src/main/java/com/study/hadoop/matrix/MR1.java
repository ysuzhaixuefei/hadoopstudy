package com.study.hadoop.matrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;

/**
 * Created by zhaixuefei on 2018/2/24.
 */
public class MR1 {
    private static String inPath = "/matrix/step1_input/matrix2.txt";
    private static String outPath = "/matrix/step1_output";
    private static String hdfs = "hdfs:zhaixuefei:9000";

    public int run(){
        //创建job配置
        try{
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS",hdfs);
            Job job = Job.getInstance(conf,"step1");
            job.setJarByClass(MR1.class);
            job.setMapperClass(Mapper1.class);
            job.setReducerClass(Reducer1.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

            FileSystem fs = FileSystem.get(conf);
            Path inputPath = new Path(inPath);
            if(fs.exists(inputPath))
                FileInputFormat.addInputPath(job,inputPath);
            Path outputPath = new Path(outPath);
            if(fs.exists(outputPath))
                fs.delete(outputPath,true);
            FileOutputFormat.setOutputPath(job,outputPath);
            return job.waitForCompletion(true)?1:-1;
        }catch(Exception e){
            e.printStackTrace();
        }
        return -1;
    }

    public static void main(String [] args){
        int result = -1;
        result = new MR1().run();
        if(result == 1)
            System.out.println("step1 success");
        else
            System.out.println("step1 failed");
    }

}
