package com.wanhao.customoutput;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/12 15:58
 *
 * @author : LiuLiHao
 * 描述：
 */
public class FilterReducer extends Reducer<Text, NullWritable,Text, NullWritable> {
    Text out = new Text();

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {
        //写出 http://www.baidu.com \r\n

        //拼接换行符
        String content = key.toString();
        content += "\r\n";

        out.set(content);

        for (NullWritable val : values) {
            context.write(out,NullWritable.get());
        }

    }
}
