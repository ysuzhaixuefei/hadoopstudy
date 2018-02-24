package com.study.hadoop.matrix;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.time.temporal.Temporal;
import java.util.Iterator;

/**
 * Created by zhaixuefei on 2018/2/24.
 */
public class Reducer1 extends Reducer<Text,Text,Text,Text>{
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for(Text text:values){
            sb.append(text + ",");
        }
        String line =  null;
        if(sb.toString().endsWith(","))
            line = sb.substring(0,sb.toString().length()-1);
        outKey.set(key);
        outKey.set(line);
        context.write(outKey,outValue);
    }

}
