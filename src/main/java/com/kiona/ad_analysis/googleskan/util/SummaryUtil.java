package com.kiona.ad_analysis.googleskan.util;

import com.kiona.ad_analysis.googleskan.constant.GoogleSkanConstant;
import com.kiona.ad_analysis.googleskan.model.DayCampaignStat;
import com.kiona.ad_analysis.googleskan.model.DayCampaignSummary;
import com.kiona.ad_analysis.googleskan.model.DayStat;
import com.kiona.ad_analysis.googleskan.model.DaySummary;
import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class SummaryUtil {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private static Date changeDate;

    static {
        try {
            changeDate = dateFormat.parse("2022-04-30");
        } catch (ParseException ignored) {
        }
    }

    public static List<DaySummary> getDaySummary(List<DayStat> dayStats){
        return dayStats.stream().map(
                dayStat -> DaySummary.builder()
                        .day(dayStat.getDay())
                        .install(dayStat.getOtherEventCount() + Arrays.stream(dayStat.getEventCounts()).sum())
                        .purchase(getPurchaseCount(dayStat.getEventCounts(), dayStat.getDay()))
                        .purchaseValue(getPurchaseValue(dayStat.getEventCounts(), dayStat.getDay()))
                        .build()
        ).collect(Collectors.toList());
    }

    public static List<DayCampaignSummary> getDayCampaignSummary(List<DayCampaignStat> dayStats){
        return dayStats.stream().map(dayStat -> DayCampaignSummary.builder()
                        .campaign(dayStat.getCampaign())
                        .day(dayStat.getDay())
                        .install(dayStat.getOtherEventCount() + Arrays.stream(dayStat.getEventCounts()).sum())
                        .purchase(getPurchaseCount(dayStat.getEventCounts(), dayStat.getDay()))
                        .purchaseValue(getPurchaseValue(dayStat.getEventCounts(), dayStat.getDay() ))
                        .build()
        ).collect(Collectors.toList());
    }

    private static int getPurchaseCount(int[] eventCounts, String day){
        int purchaseCount = 0;
        for (int eventNo = 0; eventNo < eventCounts.length; eventNo++) {
            if(isPurchaseEventNoByDay(eventNo, day)){
                purchaseCount += eventCounts[eventNo];
            }
        }
        return purchaseCount;
    }

    private static double getPurchaseValue(int[] eventCounts, String day){
        double purchaseValue = 0;
        for (int eventNo = 0; eventNo < eventCounts.length; eventNo++) {
            if(isPurchaseEventNoByDay(eventNo, day)){
                int eventCount = eventCounts[eventNo];
                Double eventValue = GoogleSkanConstant.getEventNoValuesByDay(day).get(String.valueOf(eventNo));
                purchaseValue += eventValue == null ? 0 : eventValue * eventCount;
            }
        }
        return purchaseValue;
    }

    private static boolean isPurchaseEventNoByDay(int eventNo, String day){
        return isBeforeChange(day) ? eventNo >= 47 : eventNo >= 19;
    }

    public static boolean isBeforeChange(String day){
        try {
            return dateFormat.parse(day).before(changeDate);
        } catch (ParseException e) {
            throw new RuntimeException("不能正确的解析日期：" + day);
        }
    }
}
