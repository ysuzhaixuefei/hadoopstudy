package com.study.hadoop.matrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import javax.xml.soap.Text;
import java.net.URI;

/**
 * Created by zhaixuefei on 2018/2/24.
 */
public class MR2 {
    private static String inPath = "/matrix/step2_input/matrix1.txt";
    private static String outPath = "/matrix/output";
    private static String cache = "/matrix/step1_output";//step1的输出
    private static String hdfs = "hdfs:zhaixuefei:9000";

    public int run(){
        //创建job配置
        try{
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS",hdfs);
            Job job = Job.getInstance(conf,"step2");
            //添加分布式缓存文件
            job.addCacheArchive(new URI(cache+"#matrix2"));
            job.setJarByClass(MR2.class);
            job.setMapperClass(Mapper2.class);
            job.setReducerClass(Reducer2.class);
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
        result = new MR2().run();
        if(result == 1)
            System.out.println("step2 success");
        else
            System.out.println("step2 failed");
    }
}
