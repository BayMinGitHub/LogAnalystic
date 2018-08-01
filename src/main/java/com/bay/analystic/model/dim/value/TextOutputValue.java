package com.bay.analystic.model.dim.value;

import com.bay.common.KpiType;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: 用户模块和地域信息模块输出的value数据类型
 * Author by BayMin, Date on 2018/8/2.
 */
public class TextOutputValue extends OutputValueBaseWritable {
    private String uuid;
    private String sessionId;

    @Override
    public KpiType getKpi() {
        return null;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.uuid);
        out.writeUTF(this.sessionId);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.uuid = in.readUTF();
        this.sessionId = in.readUTF();
    }

    public String getUuid() {
        return uuid;
    }

    public void setUuid(String uuid) {
        this.uuid = uuid;
    }

    public String getSessionId() {
        return sessionId;
    }

    public void setSessionId(String sessionId) {
        this.sessionId = sessionId;
    }
}
