//package com.bay.analystic.model.dim.key;
//
//import com.bay.analystic.model.dim.base.BaseDimension;
//import com.bay.analystic.model.dim.base.EventDimension;
//
//import java.io.DataInput;
//import java.io.DataOutput;
//import java.io.IOException;
//
///**
// * @Description: 封装公共维度和事件维度
// * Author by BayMin, Date on 2018/8/2.
// */
//public class StatsEventDimension extends StatsBaseDimension {
//    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
//    private EventDimension eventDimension = new EventDimension();
//
//    public StatsEventDimension() {
//    }
//
//    public StatsEventDimension(StatsCommonDimension statsCommonDimension, EventDimension eventDimension) {
//        this.statsCommonDimension = statsCommonDimension;
//        this.eventDimension = eventDimension;
//    }
//
//    @Override
//    public int compareTo(BaseDimension o) {
//        if (this == o)
//            return 0;
//        StatsEventDimension other = (StatsEventDimension) o;
//        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
//        if (tmp != 0)
//            return tmp;
//        tmp = this.eventDimension.compareTo(other.statsCommonDimension);
//        return tmp;
//
//    }
//
//    @Override
//    public void write(DataOutput out) throws IOException {
//        this.statsCommonDimension.write(out);
//        this.eventDimension.write(out);
//    }
//
//    @Override
//    public void readFields(DataInput in) throws IOException {
//        this.statsCommonDimension.readFields(in);
//        this.eventDimension.readFields(in);
//    }
//
//    @Override
//    public boolean equals(Object o) {
//        if (this == o) return true;
//        if (o == null || getClass() != o.getClass()) return false;
//
//        StatsEventDimension that = (StatsEventDimension) o;
//
//        if (!statsCommonDimension.equals(that.statsCommonDimension)) return false;
//        return eventDimension.equals(that.eventDimension);
//    }
//
//    @Override
//    public int hashCode() {
//        int result = statsCommonDimension.hashCode();
//        result = 31 * result + eventDimension.hashCode();
//        return result;
//    }
//
//    public StatsCommonDimension getStatsCommonDimension() {
//        return statsCommonDimension;
//    }
//
//    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
//        this.statsCommonDimension = statsCommonDimension;
//    }
//
//    public EventDimension getEventDimension() {
//        return eventDimension;
//    }
//
//    public void setEventDimension(EventDimension eventDimension) {
//        this.eventDimension = eventDimension;
//    }
//}
