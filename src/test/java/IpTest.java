import com.qianfneg.etl.util.IpParserUtil;
import com.qianfneg.etl.util.ip.IPSeeker;

import java.util.List;

/**
 * @Description: 测试
 * Author by BayMin, Date on 2018/7/25.
 */
public class IpTest {
    public static void main(String[] args) {
        String country = IPSeeker.getInstance().getCountry("59.67.194.5");
        int index = country.indexOf("省");
        System.out.println(country.substring(0, index + 1));
        System.out.println(IPSeeker.getInstance().getCountry("192.168.216.111"));
        List<String> ips = IPSeeker.getInstance().getAllIp();
        for (String ip : ips) {
            System.out.println(ip + "===" + new IpParserUtil().parserIp(ip));
        }
        System.out.println(new IpParserUtil().parserIp1("http://ip.taobao.com/service/getIpInfo.php?ip=" + "59.67.194.5", "UTF-8"));
    }
}

