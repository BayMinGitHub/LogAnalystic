package com.bay.analystic.model.dim.key;

import com.bay.analystic.model.dim.base.BaseDimension;
import com.bay.analystic.model.dim.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Description: 封装公共维度和地域维度
 * Author by BayMin, Date on 2018/8/1.
 */
public class StatsLocationDimension extends StatsBaseDimension {
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private LocationDimension locationDimension = new LocationDimension();

    public StatsLocationDimension() {
    }

    public StatsLocationDimension(StatsCommonDimension statsCommonDimension, LocationDimension locationDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.locationDimension = locationDimension;
    }

    /**
     * 克隆
     */
    public static StatsLocationDimension clone(StatsLocationDimension dimension) {
        StatsCommonDimension statsCommonDimension = StatsCommonDimension.clone(dimension.statsCommonDimension);
        LocationDimension locationDimension = new LocationDimension(dimension.locationDimension.getCountry(), dimension.locationDimension.getProvince(), dimension.locationDimension.getCity());
        return new StatsLocationDimension(statsCommonDimension, locationDimension);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o)
            return 0;
        StatsLocationDimension other = (StatsLocationDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if (tmp != 0)
            return tmp;
        tmp = this.locationDimension.compareTo(other.locationDimension);
        return tmp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        this.statsCommonDimension.write(out);
        this.locationDimension.write(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        StatsLocationDimension that = (StatsLocationDimension) o;

        if (!statsCommonDimension.equals(that.statsCommonDimension)) return false;
        return locationDimension.equals(that.locationDimension);
    }

    @Override
    public int hashCode() {
        int result = statsCommonDimension.hashCode();
        result = 31 * result + locationDimension.hashCode();
        return result;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.statsCommonDimension.readFields(in);
        this.locationDimension.readFields(in);
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public LocationDimension getLocationDimension() {
        return locationDimension;
    }

    public void setLocationDimension(LocationDimension locationDimension) {
        this.locationDimension = locationDimension;
    }
}
