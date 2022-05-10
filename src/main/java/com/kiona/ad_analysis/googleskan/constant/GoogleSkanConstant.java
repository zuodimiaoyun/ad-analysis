package com.kiona.ad_analysis.googleskan.constant;

import com.kiona.ad_analysis.googleskan.util.SummaryUtil;

import java.util.HashMap;
import java.util.Map;

public class GoogleSkanConstant {
    public static final String HEADER_DAY = "Day";
    public static final String HEADER_CAMPAIGN = "Campaign";
    public static final String HEADER_OTHER_EVENT = "Unavailable_SKAdNetwork Conversions";
    public static Map<String, Double> eventNoV1Values = new HashMap<>();
    public static Map<String, Double> eventNoV2Values = new HashMap<>();
    static {
        eventNoV1Values.put("47",2.99);
        eventNoV1Values.put("48",5.49);
        eventNoV1Values.put("49",7.98);
        eventNoV1Values.put("50",10.48);
        eventNoV1Values.put("51",12.98);
        eventNoV1Values.put("52",15.47);
        eventNoV1Values.put("53",17.97);
        eventNoV1Values.put("54",22.47);
        eventNoV1Values.put("55",27.47);
        eventNoV1Values.put("56",34.97);
        eventNoV1Values.put("57",45.45);
        eventNoV1Values.put("58",69.41);
        eventNoV1Values.put("59",98.91);
        eventNoV1Values.put("60",122.92);
        eventNoV1Values.put("61",162.92);
        eventNoV1Values.put("62",245.87);
        eventNoV1Values.put("63",1071.27);
        eventNoV2Values.put("19",0.99);
        eventNoV2Values.put("20",1.98);
        eventNoV2Values.put("21",2.97);
        eventNoV2Values.put("22",3.97);
        eventNoV2Values.put("23",4.99);
        eventNoV2Values.put("24",5.98);
        eventNoV2Values.put("25",6.97);
        eventNoV2Values.put("26",7.96);
        eventNoV2Values.put("27",8.95);
        eventNoV2Values.put("28",9.99);
        eventNoV2Values.put("29",10.97);
        eventNoV2Values.put("30",11.96);
        eventNoV2Values.put("31",12.95);
        eventNoV2Values.put("32",13.95);
        eventNoV2Values.put("33",14.98);
        eventNoV2Values.put("34",15.97);
        eventNoV2Values.put("35",16.96);
        eventNoV2Values.put("36",17.94);
        eventNoV2Values.put("37",18.94);
        eventNoV2Values.put("38",19.97);
        eventNoV2Values.put("39",20.96);
        eventNoV2Values.put("40",24.99);
        eventNoV2Values.put("41",29.98);
        eventNoV2Values.put("42",34.97);
        eventNoV2Values.put("43",39.97);
        eventNoV2Values.put("44",45.95);
        eventNoV2Values.put("45",55.95);
        eventNoV2Values.put("46",65.94);
        eventNoV2Values.put("47",75.92);
        eventNoV2Values.put("48",85.95);
        eventNoV2Values.put("49",95.89);
        eventNoV2Values.put("50",109.95);
        eventNoV2Values.put("51",136.85);
        eventNoV2Values.put("52",170.75);
        eventNoV2Values.put("53",200.91);
        eventNoV2Values.put("54",231.91);
        eventNoV2Values.put("55",264.84);
        eventNoV2Values.put("56",292.85);
        eventNoV2Values.put("57",315.91);
        eventNoV2Values.put("58",369.86);
        eventNoV2Values.put("59",420.86);
        eventNoV2Values.put("60",475.76);
        eventNoV2Values.put("61",548.73);
        eventNoV2Values.put("62",665.8);
        eventNoV2Values.put("63",1255.63);
    }

    public static Map<String, Double> getEventNoValuesByDay(String day){
        boolean beforeChange = SummaryUtil.isBeforeChange(day);
        return beforeChange ? eventNoV1Values : eventNoV2Values;
    }
}
