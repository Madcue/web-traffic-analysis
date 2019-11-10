package com.twq.iplocation;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

/**
 * Ip数据库来源于：https://dev.maxmind.com/geoip/legacy/geolite/#IP_Geolocation
 * 对应着三个csv文件：
 * GeoLiteCity-Blocks.csv ： 内含三个字段，起始ip,终止ip,城市id
 *      其中ip是用整形来表示的，如下：
 *          address = '174.36.207.186'
 ( o1, o2, o3, o4 ) = address.split('.')
 integer_ip = ( 256 * 256 * 256 * o1 )
 + (       256 * 256 * o2 )
 + (             256 * o3 )
 +                     o4
 GeoLiteCity-Location.csv：含有城市ip以及城市的详细信息

 region_codes.csv：含有国家、区域id以及区域名称
 */
public class IpLocationParser {
    private static Map<IpRange,Long> ip2CityIdMap = new HashMap<IpRange,Long>();
    private static Map<String ,String> regionCode2RegionName = new HashMap<>();
    private static Map<Long,IpLocation> cityId2CityMap = new HashMap<Long,IpLocation>();
    static {
        try{
            InputStream cityBlocks = IpLocationParser.class.getClassLoader()
                    .getResourceAsStream("GeoLiteCity-Block.csv");
            BufferedReader cityBlocksReader = new BufferedReader(new InputStreamReader(cityBlocks));
            String line = null;
            while((line = cityBlocksReader.readLine())!=null){
                if(line.startsWith("#")){
                    continue;
                }
                String[] temps = line.replace("\"","").split(",");
                ip2CityIdMap.put(new IpRange(Long.parseLong(temps[0]),Long.parseLong(temps[1])),Long.parseLong(temps[2]));
            }
            //解析地区名称
            InputStream regionCodes = IpLocationParser.class.getClassLoader()
                    .getResourceAsStream("region_codes.csv");
            BufferedReader regionCodesReader = new BufferedReader(new InputStreamReader(regionCodes));
            while ((line = regionCodesReader.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] temps = line.replace("\"", "").split(",");
                // key：国家-区域Id
                //value：区域名称
                regionCode2RegionName.put(temps[0] + "-" + temps[1], temps[2]);
            }

            InputStream cityLocation = IpLocationParser.class.getClassLoader()
                    .getResourceAsStream("GeoLiteCity-Location.csv");
            BufferedReader cityLocationReader = new BufferedReader(new InputStreamReader(cityLocation));
            while ((line = cityLocationReader.readLine()) != null) {
                if (line.startsWith("#")) {
                    continue;
                }
                String[] temps = line.replace("\"", "").split(",");
                IpLocation ipLocation = new IpLocation();
                ipLocation.setCountry(temps[1]);
                ipLocation.setRegion(regionCode2RegionName.getOrDefault(temps[1] + "-" + temps[2], "-"));
                ipLocation.setCity(temps[3]);
                ipLocation.setPostalCode(temps[4]);
                ipLocation.setLatitude(temps[5]);
                ipLocation.setLongitude(temps[6]);
                //key：位置id
                //value：IpLocation
                cityId2CityMap.put(Long.parseLong(temps[0]), ipLocation);
            }
        } catch (Exception e) {
            throw new RuntimeException("init ip database error", e);
        }

    }

    /**
     * 根据ip找到对应的位置信息
     * @param ip
     * @return
     */
    public static IpLocation parse(String ip) {
        Long score = ip2Score(ip);
        for (Map.Entry<IpRange, Long> entry : ip2CityIdMap.entrySet()) {
            if (score >= entry.getKey().getStartIp() && score <= entry.getKey().getEndIp()) {
                return cityId2CityMap.get(entry.getValue());
            }
        }
        return null;
    }

    /**
     * 将ip转成整型
     * @param ip
     * @return
     */
    private static Long ip2Score(String ip) {
        String[] temps = ip.split("\\.");
        Long score = 256 * 256 * 256 * Long.parseLong(temps[0]) +
                256 * 256 * Long.parseLong(temps[1]) +
                256 * Long.parseLong(temps[2]) + Long.parseLong(temps[3]);
        return score;
    }
}
