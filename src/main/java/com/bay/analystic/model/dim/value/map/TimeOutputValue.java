package com.bay.analystic.model.dim.value.map;

import com.bay.analystic.model.dim.value.OutputValueBaseWritable;
import com.bay.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: 用户模块和浏览器模块的map阶段的value输出类型
 * Author by BayMin, Date on 2018/7/27.
 */
public class TimeOutputValue extends OutputValueBaseWritable {
    private String id; // 泛指 uuid sesionid memberid
    private long time;

    @Override
    public KpiType getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.id);
        out.writeLong(this.time);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.time = in.readLong();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }
}
