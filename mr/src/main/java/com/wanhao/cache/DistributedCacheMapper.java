package com.wanhao.cache;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by LiuLiHao on 2020/10/15 8:57
 *
 * @author : LiuLiHao
 * 描述：
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    /**
     * 缓存内容
     */
    Map<String,String> map = new HashMap<>(16);

    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        //获取缓存文件路径
        String path = context.getCacheFiles()[0].getPath().toString();

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path)));

        //读内容
        String line;
        while ( (line = reader.readLine())!=null){
            String[] array = line.split("\t");
            // 01	小米
            map.put(array[0],array[1]);
        }
        reader.close();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        //1001	01	1
        String[] array = value.toString().split("\t");
        //拼接输出内容
        String content = array[0] + "\t" + map.get(array[1]) + "\t" + array[2];
        k.set(content);

        //写出
        context.write(k,NullWritable.get());
    }
}
