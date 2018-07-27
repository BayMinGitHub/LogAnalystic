package com.qianfeng.etl.mr.tohdfs;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * create external table if not exists logs(
 * s_time string,
 * en string,
 * ver string,
 * u_ud string,
 * u_mid string,
 * u_sid string,
 * c_time string,
 * language  string,
 * b_iev string,
 * b_rst string,
 * p_url string,
 * p_ref string,
 * tt string,
 * pl string,
 * o_id string,
 * `on` string,
 * cut string,
 * cua string,
 * pt string,
 * ca string,
 * ac string,
 * kv_ string,
 * du string,
 * os string,
 * os_v string,
 * browser string,
 * browser_v string,
 * country string,
 * province string,
 * city string
 * )
 * partitioned by(month String,day string)
 * row format delimited fields terminated by '\001'
 * ;
 * <p>
 * load data inpath '/gpTransform/month=12/day=11' into table logs partition(month=12,day=11);
 *
 * @Description: 自定义Writable
 * Author by BayMin, Date on 2018/7/27.
 */
public class LogDataWritable implements Writable {
    public String s_time;
    public String en;
    public String ver;
    public String u_ud;
    public String u_mid;
    public String u_sid;
    public String c_time;
    public String language = "1";
    public String b_iev;
    public String b_rst;
    public String p_url;
    public String p_ref;
    public String tt;
    public String pl;
    public String oid;
    public String on;
    public String cut;
    public String cua;
    public String pt;
    public String ca;
    public String ac;
    public String kv_;
    public String du;
    public String os;
    public String os_v;
    public String browser;
    public String browser_v;
    public String country;
    public String province;
    public String city;

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(this.s_time);
        out.writeUTF(this.en);
        out.writeUTF(this.ver);
        out.writeUTF(this.u_ud);
        out.writeUTF(this.u_mid);
        out.writeUTF(this.u_sid);
        out.writeUTF(this.c_time);
        out.writeUTF(this.language);
        out.writeUTF(this.b_iev);
        out.writeUTF(this.b_rst);
        out.writeUTF(this.p_url);
        out.writeUTF(this.p_ref);
        out.writeUTF(this.tt);
        out.writeUTF(this.pl);
        out.writeUTF(this.oid);
        out.writeUTF(this.on);
        out.writeUTF(this.cut);
        out.writeUTF(this.cua);
        out.writeUTF(this.pt);
        out.writeUTF(this.ca);
        out.writeUTF(this.ac);
        out.writeUTF(this.kv_);
        out.writeUTF(this.du);
        out.writeUTF(this.os);
        out.writeUTF(this.os_v);
        out.writeUTF(this.browser);
        out.writeUTF(this.browser_v);
        out.writeUTF(this.country);
        out.writeUTF(this.province);
        out.writeUTF(this.city);

    }

    @Override
    public void readFields(DataInput in) throws IOException {
        this.s_time = in.readUTF();
        this.en = in.readUTF();
        this.ver = in.readUTF();
        this.u_ud = in.readUTF();
        this.u_mid = in.readUTF();
        this.u_sid = in.readUTF();
        this.c_time = in.readUTF();
        this.language = in.readUTF();
        this.b_iev = in.readUTF();
        this.b_rst = in.readUTF();
        this.p_url = in.readUTF();
        this.p_ref = in.readUTF();
        this.tt = in.readUTF();
        this.pl = in.readUTF();
        this.oid = in.readUTF();
        this.on = in.readUTF();
        this.cut = in.readUTF();
        this.cua = in.readUTF();
        this.pt = in.readUTF();
        this.ca = in.readUTF();
        this.ac = in.readUTF();
        this.kv_ = in.readUTF();
        this.du = in.readUTF();
        this.os = in.readUTF();
        this.os_v = in.readUTF();
        this.browser = in.readUTF();
        this.browser_v = in.readUTF();
        this.country = in.readUTF();
        this.province = in.readUTF();
        this.city = in.readUTF();
    }

    public String getS_time() {
        return s_time;
    }

    public void setS_time(String s_time) {
        this.s_time = s_time;
    }

    public String getEn() {
        return en;
    }

    public void setEn(String en) {
        this.en = en;
    }

    public String getVer() {
        return ver;
    }

    public void setVer(String ver) {
        this.ver = ver;
    }

    public String getU_ud() {
        return u_ud;
    }

    public void setU_ud(String u_ud) {
        this.u_ud = u_ud;
    }

    public String getU_mid() {
        return u_mid;
    }

    public void setU_mid(String u_mid) {
        this.u_mid = u_mid;
    }

    public String getU_sid() {
        return u_sid;
    }

    public void setU_sid(String u_sid) {
        this.u_sid = u_sid;
    }

    public String getC_time() {
        return c_time;
    }

    public void setC_time(String c_time) {
        this.c_time = c_time;
    }

    public String getLanguage() {
        return language;
    }

    public void setLanguage(String language) {
        this.language = language;
    }

    public String getB_iev() {
        return b_iev;
    }

    public void setB_iev(String b_iev) {
        this.b_iev = b_iev;
    }

    public String getB_rst() {
        return b_rst;
    }

    public void setB_rst(String b_rst) {
        this.b_rst = b_rst;
    }

    public String getP_url() {
        return p_url;
    }

    public void setP_url(String p_url) {
        this.p_url = p_url;
    }

    public String getP_ref() {
        return p_ref;
    }

    public void setP_ref(String p_ref) {
        this.p_ref = p_ref;
    }

    public String getTt() {
        return tt;
    }

    public void setTt(String tt) {
        this.tt = tt;
    }

    public String getPl() {
        return pl;
    }

    public void setPl(String pl) {
        this.pl = pl;
    }

    public String getOid() {
        return oid;
    }

    public void setOid(String o_id) {
        this.oid = o_id;
    }

    public String getOn() {
        return on;
    }

    public void setOn(String on) {
        this.on = on;
    }

    public String getCut() {
        return cut;
    }

    public void setCut(String cut) {
        this.cut = cut;
    }

    public String getCua() {
        return cua;
    }

    public void setCua(String cua) {
        this.cua = cua;
    }

    public String getPt() {
        return pt;
    }

    public void setPt(String pt) {
        this.pt = pt;
    }

    public String getCa() {
        return ca;
    }

    public void setCa(String ca) {
        this.ca = ca;
    }

    public String getAc() {
        return ac;
    }

    public void setAc(String ac) {
        this.ac = ac;
    }

    public String getKv_() {
        return kv_;
    }

    public void setKv_(String kv_) {
        this.kv_ = kv_;
    }

    public String getDu() {
        return du;
    }

    public void setDu(String du) {
        this.du = du;
    }

    public String getOs() {
        return os;
    }

    public void setOs(String os) {
        this.os = os;
    }

    public String getOs_v() {
        return os_v;
    }

    public void setOs_v(String os_v) {
        this.os_v = os_v;
    }

    public String getBrowser() {
        return browser;
    }

    public void setBrowser(String browser) {
        this.browser = browser;
    }

    public String getBrowser_v() {
        return browser_v;
    }

    public void setBrowser_v(String browser_v) {
        this.browser_v = browser_v;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return s_time + "\u0001" + en + "\u0001"
                + ver + "\u0001" + u_ud + "\u0001" + u_mid + "\u0001"
                + u_sid + "\u0001" + c_time + "\u0001" + language
                + "\u0001" + b_iev + "\u0001" + b_rst + "\u0001" + p_url
                + "\u0001" + p_ref + "\u0001" + tt + "\u0001" + pl + "\u0001"
                + oid + "\u0001" + on + "\u0001" + cut + "\u0001" + cua
                + "\u0001" + pt + "\u0001" + ca + "\u0001" + ac + "\u0001" + kv_
                + "\u0001" + du + "\u0001" + os + "\u0001" + os_v + "\u0001"
                + browser + "\u0001" + browser_v + "\u0001" + country
                + "\u0001" + province + "\u0001" + city;
    }
}
