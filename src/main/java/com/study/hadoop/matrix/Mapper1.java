package com.study.hadoop.matrix;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by zhaixuefei on 2018/2/24.
 */
public class Mapper1 extends Mapper<LongWritable,Text,Text,Text>{
    private Text outKey = new Text();
    private Text outValue = new Text();

    /**
     * @param key 1
     * @param value 1	1_0,2_3,3_-1,4_2,5_-3
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key,Text value,Context context) throws IOException, InterruptedException {
        String [] rowAndLine = value.toString().split("\t");
        String row = rowAndLine[0];//矩阵的行号

        String [] lines = rowAndLine[1].split(",");
        for(int i=0;i<lines.length;i++){
            String column = lines[i].split("_")[0];
            String valueStr = lines[i].split("_")[1];
            outKey.set(column);
            outValue.set(row+"_"+valueStr);
            context.write(outKey,outValue);
        }


    }

}
