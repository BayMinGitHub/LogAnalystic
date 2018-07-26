import com.qianfneg.etl.util.LogUtil;

import java.util.Map;

/**
 * @Description: 日志工具类测试
 * Author by BayMin, Date on 2018/7/26.
 */
public class LogUtilTest {
    public static void main(String[] args) {
        Map<String, String> maps = new LogUtil().parserLog("114.61.94.253^A1531189702.375^Ahh^A/BCImg.gif?en=e_pv&p_url=http%3A%2F%2Flocalhost%3A8080%2Fbf_track_jssdk%2Fdemo.jsp&p_ref=http%3A%2F%2Flocalhost%3A8080%2Fbf_track_jssdk%2Fdemo.jsp&tt=%E6%B5%8B%E8%AF%95%E9%A1%B5%E9%9D%A21&ver=1&pl=website&sdk=js&u_ud=D4289356-5BC9-47C4-8F7D-F16022833E7E&u_sd=FB47F1DE-2C1B-4F41-8C38-344040ABCCA0&c_time=1495462088375&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F46.0.2490.71%20Safari%2F537.36&b_rst=1280*768");
        for (Map.Entry<String, String> en : maps.entrySet()) {
            System.out.println(en.getKey() + ":" + en.getValue());
        }
    }
}
