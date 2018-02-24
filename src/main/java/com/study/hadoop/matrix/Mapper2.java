package com.study.hadoop.matrix;

import org.apache.hadoop.hdfs.protocolPB.JournalProtocolTranslatorPB;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhaixuefei on 2018/2/24.
 */
public class Mapper2  extends Mapper<LongWritable,Text,Text,Text>{
    private Text outKey = new Text();
    private Text outValue = new Text();
    private List<String> cateList = new ArrayList<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        //通过输入流将全局缓存中的右侧矩阵读入List<String>
        FileReader fr = new FileReader("matrix2");
        BufferedReader br = new BufferedReader(fr);

        String line = null;
        //每行格式 行 tab列 列_值,列_值
        while((line = br.readLine())!=null){
            cateList.add(line);
        }
        fr.close();
        br.close();
    }

    /**
     * @param key 1 行号
     * @param value 1_0,1_3,1_-1,1_2,1_-3
     * @param context
     * @throws IOException
     * @throws InterruptedException
     */
    @Override
    protected void map(LongWritable key, Text value, Mapper.Context context) throws IOException, InterruptedException {
        String row_matrix = value.toString().split("\t")[0];
        String [] column_value = value.toString().split("\t")[1].split(",");
        for(String line : cateList){
            String row_matrix2 = line.split("\t")[0];
            String column_value2 [] = line.toString().split("\t")[1].split(",");
            //矩阵两行相乘
            int result = 0;
            //遍历左矩阵第一行的每一列
            for(String temp:column_value){
                String column_number = temp.split(",")[0];
                String column_Array_value = temp.split(",")[1];
                for(String temp1:column_value2){
                    //如果右侧矩阵的列和左侧矩阵的列是相等的
                    if(temp1.startsWith(column_number)){
                        String value2 = temp1.split(",")[1];
                        result += Integer.parseInt(value2)*Integer.parseInt(column_Array_value);
                    }
                }
            }
            outKey.set(row_matrix2);
            outValue.set(row_matrix2+"_"+result);
            context.write(outKey, outValue);
        }
    }
}
