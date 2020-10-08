package com.wanhao.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * Created by LiuLiHao on 2020/10/8 10:43
 *
 * @author : LiuLiHao
 * 描述：自定义分区
 */
public class FlowPartition extends Partitioner<Text,FlowBean> {

    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        String prefix = text.toString().substring(0, 3);
        switch (prefix){
            case "136":
                return 0;
            case "137":
                return 1;
            case "138":
                return 2;
            case "139":
                return 3;
            default:
                return 4;
        }
    }
}
