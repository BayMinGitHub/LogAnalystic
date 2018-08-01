import com.bay.analystic.model.dim.base.PlatFormDimension;
import com.bay.analystic.service.IDimensionConvert;
import com.bay.analystic.service.impl.IDimensionConvertImpl;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @Description: 维度类测试
 * Author by BayMin, Date on 2018/7/30.
 */
public class DimensionTest {
    public static void main(String[] args) {
        IDimensionConvert convert = new IDimensionConvertImpl();
        PlatFormDimension pl = new PlatFormDimension("www");
        try {
            System.out.println(convert.getDimensionIDByDimension(pl));
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
