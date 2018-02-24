package com.study.hadoop.matrix;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by zhaixuefei on 2018/2/24.
 */
public class Reducer2 extends Reducer<Text,Text,Text,Text> {
    private Text outKey = new Text();
    private Text outValue = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for(Text text : values){
            sb.append(text.toString()+",");
        }
        String result = null;
        if(sb.toString().endsWith(","));
            result = sb.toString().substring(0,sb.toString().length()-1);
        outKey.set(key);
        outValue.set(result);
        context.write(outKey,outValue);
    }
}

