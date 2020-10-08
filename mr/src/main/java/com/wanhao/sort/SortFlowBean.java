package com.wanhao.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by LiuLiHao on 2020/10/6 16:24
 * @author : LiuLiHao
 * 描述：带排序对象
 */
public class SortFlowBean implements WritableComparable<SortFlowBean> {

    private long upFlow;
    private long downFlow;
    private long sumFlow;

    public SortFlowBean() {
        super();
    }

    public SortFlowBean(long upFlow, long downFlow) {
        super();
        this.upFlow =  upFlow;
        this.downFlow =  downFlow;
        this.sumFlow = upFlow+downFlow;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(upFlow);
        dataOutput.writeLong(downFlow);
        dataOutput.writeLong(sumFlow);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        upFlow = dataInput.readLong();
        downFlow = dataInput.readLong();
        sumFlow = dataInput.readLong();
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow ;
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public int compareTo(SortFlowBean o) {
        if (sumFlow > o.sumFlow) {
            return -1;
        } else if (sumFlow < o.sumFlow) {
            return 1;
        }
        return 0;
    }
}
