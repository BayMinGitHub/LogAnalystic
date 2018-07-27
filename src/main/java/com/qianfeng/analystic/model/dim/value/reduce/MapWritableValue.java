package com.qianfeng.analystic.model.dim.value.reduce;

import com.qianfeng.analystic.model.dim.value.OutputValueBaseWritable;
import com.qianfeng.common.KpiType;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: 用户模块和浏览器模块reduce阶段下的value输出的类
 * Author by BayMin, Date on 2018/7/27.
 */
public class MapWritableValue extends OutputValueBaseWritable {
    private MapWritable value = new MapWritable();
    // MapWritable可以带多个属性,比较方便
    // 1.newUser     private int newUser;
    // 2.totalUser   private int totalUser;
    private KpiType kpi;

    public MapWritableValue() {
    }

    public MapWritableValue(MapWritable value, KpiType kpi) {
        this.value = value;
        this.kpi = kpi;
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.value.write(out); // MapWritable的写出
        WritableUtils.writeEnum(out, kpi); // 注意枚举的写出
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.value.readFields(in);
        WritableUtils.readEnum(in, KpiType.class);
    }

    public MapWritable getValue() {
        return value;
    }

    public void setValue(MapWritable value) {
        this.value = value;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }
}
