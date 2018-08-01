package com.bay.analystic.model.dim.value;

import com.bay.common.KpiType;
import org.apache.hadoop.io.WritableUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: 地域模块reduce阶段输出的value类型
 * Author by BayMin, Date on 2018/8/2.
 */
public class LocationReducerOutputWritable extends OutputValueBaseWritable {
    private KpiType kpi;
    private int activeUsers; // 活跃用户个数
    private int sessions; // 会话个数
    private int bounceSessions; // 跳出会话个数

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.activeUsers);
        out.writeInt(this.sessions);
        out.writeInt(this.bounceSessions);
        WritableUtils.writeEnum(out, this.kpi);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.activeUsers = in.readInt();
        this.sessions = in.readInt();
        this.bounceSessions = in.readInt();
        WritableUtils.readEnum(in, KpiType.class);
    }

    @Override
    public KpiType getKpi() {
        return this.kpi;
    }

    public void setKpi(KpiType kpi) {
        this.kpi = kpi;
    }

    public int getActiveUsers() {
        return activeUsers;
    }

    public void setActiveUsers(int activeUsers) {
        this.activeUsers = activeUsers;
    }

    public int getSessions() {
        return sessions;
    }

    public void setSessions(int sessions) {
        this.sessions = sessions;
    }

    public int getBounceSessions() {
        return bounceSessions;
    }

    public void setBounceSessions(int bounceSessions) {
        this.bounceSessions = bounceSessions;
    }
}
