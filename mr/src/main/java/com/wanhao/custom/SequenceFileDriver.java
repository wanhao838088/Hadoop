package com.wanhao.custom;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

/**
 * Created by LiuLiHao on 2020/10/7 20:18
 *
 * @author : LiuLiHao
 * 描述：
 */
public class SequenceFileDriver {
    public static void main(String[] args) throws Exception{
        args = new String[] { "D:/input/custom", "D:/output1" };

        // 获取job对象
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置jar包存储位置、关联自定义的mapper和reducer
        job.setJarByClass(SequenceFileDriver.class);
        job.setMapperClass(SequenceFileMapper.class);
        job.setReducerClass(SequenceFileReducer.class);

        // 设置输入的inputFormat
        job.setInputFormatClass(WholeFileInputFormat.class);

        // 设置输出的outputFormat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        //  设置map输出端的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        // 设置最终输出端的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交job
        System.out.println(job.waitForCompletion(true));
    }
}
