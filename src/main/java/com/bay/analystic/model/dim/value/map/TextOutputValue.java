package com.bay.analystic.model.dim.value.map;

import com.bay.analystic.model.dim.value.OutputValueBaseWritable;
import com.bay.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: 地域信息模块map输出的value数据类型
 * Author by BayMin, Date on 2018/8/2.
 */
public class TextOutputValue extends OutputValueBaseWritable {
    private String uuid;
    private String item; // sessionId,event

    @Override
    public KpiType getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.uuid);
        out.writeUTF(this.item);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uuid = in.readUTF();
        this.item = in.readUTF();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String itemId) {
        this.item = itemId;
    }
}
