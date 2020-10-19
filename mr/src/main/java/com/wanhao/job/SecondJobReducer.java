package com.wanhao.job;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/19 9:35
 *
 * @author : LiuLiHao
 * 描述：
 */
public class SecondJobReducer extends Reducer<Text,Text,Text, Text> {
    Text v = new Text();

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        //输入内容
        /*
        aaa  a.txt \t 3
             b.txt \t 2
             c.txt \t 1
         */
        StringBuilder sb = new StringBuilder();

        for (Text value : values) {
            //a.txt-->3 \t
            sb.append(value.toString().replaceAll("\t", "-->")).append("\t");
        }

        v.set(sb.toString());
        context.write(key,v);

        //输出内容  aaa  a.txt-->3 b.txt-->2 c.txt-->1
    }
}
