package com.wanhao.keyvalueformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by LiuLiHao on 2020/10/7 11:31
 *
 * @author : LiuLiHao
 * 描述：
 */
public class KVDriver {
    public static void main(String[] args) throws Exception {
        args = new String[]{"D:/input/kv","D:/output"};

        Configuration configuration = new Configuration();
        //设置分隔符
        configuration.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR," ");
        Job job = Job.getInstance(configuration);

        job.setJarByClass(KVDriver.class);
        //mapper class
        job.setMapperClass(KVMapper.class);
        //reduce class
        job.setReducerClass(KVReducer.class);

        //map输出类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //最终输出类
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(KeyValueTextInputFormat.class);
        //输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交任务
        System.out.println(job.waitForCompletion(true));

    }
}
