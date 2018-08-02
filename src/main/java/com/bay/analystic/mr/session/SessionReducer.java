package com.bay.analystic.mr.session;

import com.bay.analystic.model.dim.key.StatsUserDimension;
import com.bay.analystic.model.dim.value.reduce.MapWritableValue;
import com.bay.analystic.model.dim.value.map.TimeOutputValue;
import com.bay.common.DateEnum;
import com.bay.common.GlobalConstants;
import com.bay.common.KpiType;
import com.bay.util.TimeUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * @Description: 新增的用户和新增的总用户统计的Reducer类
 * Author by BayMin, Date on 2018/7/27.
 */
public class SessionReducer extends Reducer<StatsUserDimension, TimeOutputValue, StatsUserDimension, MapWritableValue> {
    // 用来去重
    private MapWritableValue v = new MapWritableValue();

    /**
     * 2018-07-26 website 111 123
     * 2018-07-26 website 111 125
     * 2018-07-26 website 112 133
     * <p>
     * 2018-07-26 website List((111,123),(111,123),(112,133))
     * <p>
     * 使用Map
     * 111-List(123,125,110,111)
     */
    private Map<String, List<Long>> map = new HashMap<>(); // sessionId-serverTime
    private Map<Integer, List<Long>> countMap = new HashMap<>(); // hour-serverTime

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        // 清空map
        this.map.clear();
        this.countMap.clear();
        // 循环给计数map赋值
        for (int i = 1; i < 25; i++) {
            countMap.put(i, new ArrayList<>());
        }
        // 循环map阶段传过来的value
        for (TimeOutputValue tv : values) {
            // 将sessionId取出来添加到set中
            String sessionId = tv.getId();
            long serverTime = tv.getTime();
            List<Long> mapList = new ArrayList<>();
            List<Long> CountMapList = new ArrayList<>();
            // 存储时间
            if (map.containsKey(sessionId)) {
                mapList = map.get(sessionId);
                mapList.add(serverTime);
                map.put(sessionId, mapList);
            } else {
                mapList.add(serverTime);
                map.put(sessionId, mapList);
            }
            int hour = TimeUtil.getDateInfo(serverTime, DateEnum.HOUR) + 1;
            CountMapList = countMap.get(hour);
            CountMapList.add(serverTime);
            countMap.put(hour, CountMapList);
        }
        // 计算session的时长
        int sessionLength = 0;
        // 构建输出的value
        // 每小时会话个数
        MapWritable mapWritable = new MapWritable();
        this.v.setKpi(KpiType.HOURLY_SESSION);
        for (Map.Entry<Integer, List<Long>> en : countMap.entrySet()) {
            mapWritable.put(new IntWritable(-en.getKey()), new IntWritable(en.getValue().size()));
        }
        this.v.setValue(mapWritable);
        context.write(key, this.v);
        // 每小时会话长度
        this.v.setKpi(KpiType.HOURLY_SESSION_LENGTH);
        for (Map.Entry<Integer, List<Long>> en : countMap.entrySet()) {
            List<Long> list = en.getValue();
            if (list.size() >= 2) {
                Collections.sort(list);
                sessionLength += (list.get(list.size() - 1) - list.get(0));
            }
            if (sessionLength > 0 && sessionLength <= GlobalConstants.DAY_OF_MILISECONDS) {
                // 不足一秒算一秒
                if (sessionLength % 1000 == 0) {
                    sessionLength = sessionLength / 1000;
                } else {
                    sessionLength = sessionLength / 1000 + 1;
                }
            }
            mapWritable.put(new IntWritable(-en.getKey()), new IntWritable(sessionLength));
        }
        this.v.setValue(mapWritable);
        context.write(key, this.v);
        // 会话长度
        sessionLength = 0;
        for (Map.Entry<String, List<Long>> en : map.entrySet()) {
            List<Long> list = en.getValue();
            if (list.size() >= 2) {
                Collections.sort(list);
                sessionLength += (list.get(list.size() - 1) - list.get(0));
            }
        }
        if (sessionLength > 0 && sessionLength <= GlobalConstants.DAY_OF_MILISECONDS) {
            // 不足一秒算一秒
            if (sessionLength % 1000 == 0) {
                sessionLength = sessionLength / 1000;
            } else {
                sessionLength = sessionLength / 1000 + 1;
            }
        }
        mapWritable.put(new IntWritable(-1), new IntWritable(this.map.size()));
        mapWritable.put(new IntWritable(-2), new IntWritable(sessionLength));
        // 设置kpi
        this.v.setValue(mapWritable);
        this.v.setKpi(KpiType.valueOfType(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        // 输出
        context.write(key, this.v);
    }
}
