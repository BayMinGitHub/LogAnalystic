import com.qianfneg.etl.util.UserAgentUtil;

/**
 * @Description: 浏览器信息解析测试类
 * Author by BayMin, Date on 2018/7/25.
 */
public class UserAgentTest {
    public static void main(String[] args) {
        // 在浏览器控制台输入 window.navigator.userAgent
        // Chrome
        System.out.println(new UserAgentUtil().parserUserAgent("Mozilla/5.0 (Linux; Android 6.0; Nexus 5 Build/MRA58N) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Mobile Safari/537.36"));
        // IE
        System.out.println(new UserAgentUtil().parserUserAgent("Mozilla/4.0 (compatible;MSIE 8.0; Windows NT 6.1; WOW64; Trident/4.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0)"));
        // EDGE,因为包太老,无法识别EDGE,所以识别为Chrome
        System.out.println(new UserAgentUtil().parserUserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.140 Safari/537.36 Edge/17.17134"));
    }
}
