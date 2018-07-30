package com.qianfeng.analystic.model.dim.key;

import com.qianfeng.analystic.model.dim.base.BaseDimension;
import com.qianfeng.analystic.model.dim.base.BrowserDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: 封装用户模块和浏览器模块中Map和Reduce阶段输出的key的类型
 * Author by BayMin, Date on 2018/7/27.
 */
public class StatsUserDimension extends StatsBaseDimension {
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private BrowserDimension browserDimension = new BrowserDimension();

    public StatsUserDimension() {
    }

    public StatsUserDimension(StatsCommonDimension statsCommonDimension, BrowserDimension browserDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.browserDimension = browserDimension;
    }

    /**
     * 克隆当前对象的一个实例
     */
    public static StatsUserDimension clone(StatsUserDimension userDimension) {
        StatsCommonDimension statsCommonDimension = StatsCommonDimension.clone(userDimension.statsCommonDimension);
        BrowserDimension browserDimension = new BrowserDimension(userDimension.browserDimension.getId(), userDimension.browserDimension.getBrowserName(), userDimension.browserDimension.getBrowserVersion());
        return new StatsUserDimension(statsCommonDimension, browserDimension);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatsUserDimension other = (StatsUserDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if (tmp != 0)
            return tmp;
        tmp = this.browserDimension.compareTo(other.browserDimension);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommonDimension.write(out);
        this.browserDimension.write(out);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommonDimension.readFields(in);
        this.browserDimension.readFields(in);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsUserDimension that = (StatsUserDimension) o;

        if (!statsCommonDimension.equals(that.statsCommonDimension)) return false;
        return browserDimension.equals(that.browserDimension);
    }

    @Override
    public int hashCode() {
        int result = statsCommonDimension.hashCode();
        result = 31 * result + browserDimension.hashCode();
        return result;
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public BrowserDimension getBrowserDimesion() {
        return browserDimension;
    }

    public void setBrowserDimesion(BrowserDimension browserDimesion) {
        this.browserDimension = browserDimesion;
    }
}
