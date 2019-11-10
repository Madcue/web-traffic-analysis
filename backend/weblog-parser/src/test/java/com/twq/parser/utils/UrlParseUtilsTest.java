package com.twq.parser.utils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.*;

public class UrlParseUtilsTest {

    @Test
    public void decode() {
        Assert.assertEquals("https://www.underarmour.cn/",
                UrlParseUtils.decode("https%3A%2F%2Fwww.underarmour.cn%2F"));
        Assert.assertEquals("Under Armour|安德玛中国官网 - UA运动品牌专卖，美国高端运动科技品牌",
                UrlParseUtils.decode("Under%20Armour%7C%E5%AE%89%E5%BE%B7%E7%8E%9B%E4%B8%AD%E5%9B%BD%E5%AE%98%E7%BD%91%20-%20UA%E8%BF%90%E5%8A%A8%E5%93%81%E7%89%8C%E4%B8%93%E5%8D%96%EF%BC%8C%E7%BE%8E%E5%9B%BD%E9%AB%98%E7%AB%AF%E8%BF%90%E5%8A%A8%E7%A7%91%E6%8A%80%E5%93%81%E7%89%8C"));
        //需要二次解码
        Assert.assertEquals("https://www.underarmour.cn/?utm_source=baidu&utm_term=标题&utm_medium=BrandZonePC&utm_channel=SEM",
                UrlParseUtils.decode("https%3A%2F%2Fwww.underarmour.cn%2F%3Futm_source%3Dbaidu%26utm_term%3D%25E6%25A0%2587%25E9%25A2%2598%26utm_medium%3DBrandZonePC%26utm_channel%3DSEM"));
        //编码后不完整的
        Assert.assertEquals("https://www.underarmour.cn/cmens-footwear-running/?utm_source=baidu&utm_campaign=PC",
                UrlParseUtils.decode("https%3A%2F%2Fwww.underarmour.cn%2Fcmens-footwear-running%2F%3Futm_source%3Dbaidu%26utm_campaign%3DPC%2"));

    }

    @Test
    public void getInfoFromUrl_1() {
        //规则的url，含有fragment，且fragment在query的后面
        String url = "https://www.underarmour.cn/s-HOVR?qf=11-149&pf=&sortStr=&nav=640#NewLaunch";
        UrlInfo info = UrlParseUtils.getInfoFromUrl(url);
        Assert.assertEquals("qf=11-149&pf=&sortStr=&nav=640", info.getQuery());
        Assert.assertEquals("https", info.getScheme());
        Assert.assertEquals("www.underarmour.cn", info.getDomain());
        Assert.assertEquals("/s-HOVR", info.getPath());
        Assert.assertEquals("NewLaunch", info.getFragment());
    }

    @Test
    public void getInfoFromUrl_2() {
        //规则的url，含有fragment，且fragment在query的前面
        String url = "https://www.underarmour.cn/s-HOVR#NewLaunch?qf=11-149&pf=&sortStr=&nav=640";
        UrlInfo info = UrlParseUtils.getInfoFromUrl(url);
        Assert.assertEquals("qf=11-149&pf=&sortStr=&nav=640", info.getQuery());
        Assert.assertEquals("https", info.getScheme());
        Assert.assertEquals("www.underarmour.cn", info.getDomain());
        Assert.assertEquals("/s-HOVR", info.getPath());
        Assert.assertEquals("NewLaunch", info.getFragment());
    }

    public void getInfoFromUrl_3() {
        //不规则的url，含有fragment，但是不含有query
        String url = "https://www.underarmour.cn/s-HOVR#44-1|NewLaunch|HOVR|HOVR|HOVR|201800607";
        UrlInfo info = UrlParseUtils.getInfoFromUrl(url);
        Assert.assertEquals("-", info.getQuery());
        Assert.assertEquals("https", info.getScheme());
        Assert.assertEquals("www.underarmour.cn", info.getDomain());
        Assert.assertEquals("/s-HOVR", info.getPath());
        Assert.assertEquals("44-1|NewLaunch|HOVR|HOVR|HOVR|201800607", info.getFragment());
    }

    @Test
    public void getInfoFromUrl_4() {
        //不规则的url，含有fragment,含有query
        String url = "https://www.underarmour.cn/s-HOVR?qf=11-149&pf=&sortStr=&nav=640#44-1|NewLaunch|HOVR|HOVR|HOVR|201800607";
        UrlInfo info = UrlParseUtils.getInfoFromUrl(url);
        Assert.assertEquals("qf=11-149&pf=&sortStr=&nav=640", info.getQuery());
        Assert.assertEquals("https", info.getScheme());
        Assert.assertEquals("www.underarmour.cn", info.getDomain());
        Assert.assertEquals("/s-HOVR", info.getPath());
        Assert.assertEquals("44-1|NewLaunch|HOVR|HOVR|HOVR|201800607", info.getFragment());
    }

    @Test
    public void getInfoFromUrl_5() {
        //不规则的url的解析, 不含有fragment
        String url = "http://m.baidu.com:8080/from=1012637v/pu=sz%401320_480%2Ccuid%40_PHful8jS8_MuvtqgaHai_iaHalh8vi20aHda_OD2a8Euv8xga-18_uQvt_Ra2tDA%2Ccua%40_a-qi4ujvfg4NE6pI5me6NIy2IgUI2tYAC_uB%2Ccut%405kSYMltqeupciXM9ravjh_h0vCgcuDPWpi3pur_aC%2Cosname%40baiduboxapp%2Cctv%402%2Ccfrom%401012637v%2Ccen%40cuid_cua_cut%2Ccsrc%40app_mainbox_txt%2Cvmgdb%400020100228y/s?tn=zbios&word=%E4%B8%87%E8%89%BE%E5%8F%AF%E4%BB%8";
        UrlInfo info = UrlParseUtils.getInfoFromUrl(url);
        Assert.assertEquals("tn=zbios&word=%E4%B8%87%E8%89%BE%E5%8F%AF%E4%BB%8", info.getQuery());
        Assert.assertEquals("http", info.getScheme());
        Assert.assertEquals("m.baidu.com:8080", info.getDomain());
        Assert.assertEquals("/from=1012637v/pu=sz@1320_480,cuid@_PHful8jS8_MuvtqgaHai_iaHalh8vi20aHda_OD2a8Euv8xga-18_uQvt_Ra2tDA,cua@_a-qi4ujvfg4NE6pI5me6NIy2IgUI2tYAC_uB,cut@5kSYMltqeupciXM9ravjh_h0vCgcuDPWpi3pur_aC,osname@baiduboxapp,ctv@2,cfrom@1012637v,cen@cuid_cua_cut,csrc@app_mainbox_txt,vmgdb@0020100228y/s", info.getPath());
    }

    @Test
    public void getQueryParams() {
        Map<String, String> map = UrlParseUtils.getQueryParams("qf=11-149&pf=&sortStr=&nav=640");
        Assert.assertEquals(4, map.size());
        Assert.assertEquals("11-149", map.get("qf"));
        Assert.assertEquals("640", map.get("nav"));
        Assert.assertEquals("-", map.get("pf"));
    }
}