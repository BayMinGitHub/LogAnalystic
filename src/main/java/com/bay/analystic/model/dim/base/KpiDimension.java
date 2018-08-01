package com.bay.analystic.model.dim.base;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: KPI(关键业绩指标)的维度, 按小时的活跃用户, 小时的session个数
 * Author by BayMin, Date on 2018/7/27.
 */
public class KpiDimension extends BaseDimension {
    private int id;
    private String kpiName;

    public KpiDimension() {
    }

    public KpiDimension(String kpiName) {
        this.kpiName = kpiName;
    }

    public KpiDimension(int id, String kpiName) {
        this(kpiName);
        this.id = id;
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (o == this)
            return 0;
        KpiDimension other = (KpiDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0)
            return tmp;
        tmp = this.kpiName.compareTo(other.kpiName);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeUTF(this.kpiName);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readInt();
        this.kpiName = in.readUTF();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        KpiDimension that = (KpiDimension) o;

        if (id != that.id) return false;
        return kpiName.equals(that.kpiName);
    }

    @Override
    public int hashCode() {
        int result = id;
        result = 31 * result + kpiName.hashCode();
        return result;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getKpiName() {
        return kpiName;
    }

    public void setKpiName(String kpiName) {
        this.kpiName = kpiName;
    }
}
