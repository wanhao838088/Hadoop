package com.wanhao.nlineformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by LiuLiHao on 2020/10/7 11:56
 *
 * @author : LiuLiHao
 * 描述：
 */
public class NLineDriver {
    public static void main(String[] args) throws Exception {
        args = new String[]{"D:/input/nline","D:/output"};
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);
        //设置切分行数
        NLineInputFormat.setNumLinesPerSplit(job,3);

        job.setJarByClass(NLineDriver.class);
        //mapper class
        job.setMapperClass(NLineMapper.class);
        //reduce class
        job.setReducerClass(NLineReducer.class);

        //map输出类
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //最终输出类
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setInputFormatClass(NLineInputFormat.class);
        //输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        //提交任务
        System.out.println(job.waitForCompletion(true));

    }
}
