package com.wanhao.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * Created by LiuLiHao on 2020/10/19 9:41
 *
 * @author : LiuLiHao
 * 描述：
 */
public class SecondJobDriver {
    public static void main(String[] args) throws Exception{

        // 输入输出路径需要根据自己电脑上实际的输入输出路径设置
        args = new String[] { "d:/output5", "d:/output6" };

        Configuration config = new Configuration();
        Job job = Job.getInstance(config);

        job.setJarByClass(SecondJobDriver.class);
        job.setMapperClass(SecondJobMapper.class);
        job.setReducerClass(SecondJobReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.out.println(job.waitForCompletion(true));
    }
}
