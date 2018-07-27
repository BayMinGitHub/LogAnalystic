package com.qianfeng.analystic.model.dim.key;

import com.qianfeng.analystic.model.dim.base.DateDimension;
import com.qianfeng.analystic.model.dim.base.KpiDimension;
import com.qianfeng.analystic.model.dim.base.PlatFormDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: 公共维度的封装(平台和时间维度)
 * Author by BayMin, Date on 2018/7/27.
 */
public class StatsCommonDimension extends StatsBaseDimension {
    private PlatFormDimension platFormDimension = new PlatFormDimension();
    private DateDimension dateDimension = new DateDimension();
    private KpiDimension kpiDimension = new KpiDimension();

    public StatsCommonDimension() {
    }

    public StatsCommonDimension(PlatFormDimension platFormDimension, DateDimension dateDimension, KpiDimension kpiDimension) {
        this.platFormDimension = platFormDimension;
        this.dateDimension = dateDimension;
        this.kpiDimension = kpiDimension;
    }

    /**
     * 克隆当前对象的一个实例
     */
    public static StatsCommonDimension clone(StatsCommonDimension commonDimension) {
        PlatFormDimension platFormDimension = new PlatFormDimension(
                commonDimension.platFormDimension.getId(),
                commonDimension.platFormDimension.getPlatformName());
        DateDimension dateDimension = new DateDimension(
                commonDimension.dateDimension.getId(),
                commonDimension.dateDimension.getYear(),
                commonDimension.dateDimension.getSeason(),
                commonDimension.dateDimension.getMonth(),
                commonDimension.dateDimension.getWeeek(),
                commonDimension.dateDimension.getDay(),
                commonDimension.dateDimension.getCalendar(),
                commonDimension.dateDimension.getType());
        KpiDimension kpiDimension = new KpiDimension(
                commonDimension.platFormDimension.getId(),
                commonDimension.platFormDimension.getPlatformName()
        );
        return new StatsCommonDimension(platFormDimension, dateDimension, kpiDimension);
    }

    @Override
    public int compareTo(StatsBaseDimension o) {
        if (this == o)
            return 0;
        StatsCommonDimension other = (StatsCommonDimension) o;
        int tmp = this.dateDimension.compareTo(other.dateDimension);
        if (tmp != 0)
            return tmp;
        tmp = this.platFormDimension.compareTo(other.platFormDimension);
        if (tmp != 0)
            return tmp;
        tmp = this.kpiDimension.compareTo(other.kpiDimension);
        return tmp;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.platFormDimension.write(out);
        this.dateDimension.write(out);
        this.kpiDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.platFormDimension.readFields(in);
        this.dateDimension.readFields(in);
        this.kpiDimension.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsCommonDimension that = (StatsCommonDimension) o;

        if (!platFormDimension.equals(that.platFormDimension)) return false;
        if (!dateDimension.equals(that.dateDimension)) return false;
        return kpiDimension.equals(that.kpiDimension);
    }

    @Override
    public int hashCode() {
        int result = platFormDimension.hashCode();
        result = 31 * result + dateDimension.hashCode();
        result = 31 * result + kpiDimension.hashCode();
        return result;
    }

    public PlatFormDimension getPlatFormDimension() {
        return platFormDimension;
    }

    public void setPlatFormDimension(PlatFormDimension platFormDimension) {
        this.platFormDimension = platFormDimension;
    }

    public DateDimension getDateDimension() {
        return dateDimension;
    }

    public void setDateDimension(DateDimension dateDimension) {
        this.dateDimension = dateDimension;
    }

    public KpiDimension getKpiDimension() {
        return kpiDimension;
    }

    public void setKpiDimension(KpiDimension kpiDimension) {
        this.kpiDimension = kpiDimension;
    }
}
