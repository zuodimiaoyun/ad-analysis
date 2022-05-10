package com.kiona.ad_analysis.googleskan.util;

import com.kiona.ad_analysis.googleskan.handler.CsvDataHandler;
import com.kiona.ad_analysis.googleskan.model.DayCampaignStat;
import com.kiona.ad_analysis.googleskan.model.DayStat;
import com.kiona.ad_analysis.googleskan.model.Stat;
import com.kiona.ad_analysis.googleskan.constant.GoogleSkanConstant;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@Slf4j
public class CsvDataParser {

    private static final Pattern eventHeaderPattern = Pattern.compile("(\\d+)_SKAdNetwork Conversions");

    @SuppressWarnings("unchecked")
    public static <T extends Stat> List<T> parseToDayStat(List<List<String>> csvData, boolean hasCampaign){
        CsvDataHandler.HeaderIndex headerIndex = getHeaderIndex(csvData, hasCampaign);
        csvData.remove(0);
        List<Stat> dayStats = new ArrayList<>();
        for (int row = 0; row < csvData.size(); row++) {
            List<String> rowData = csvData.get(row);
            int columnSize = rowData.size();
            int rowNum = row + 1;
            boolean checkSuccess = checkColumns(rowNum, columnSize, headerIndex, hasCampaign);
            if(!checkSuccess){
                log.warn("第" + rowNum + "行，存在不可解析数据，跳过！！！！！");
                continue;
            }
            String day = rowData.get(headerIndex.getDayIndex());
            int otherEventCount = Integer.parseInt(rowData.get(headerIndex.getOtherEventIndex()));
            String campaign = hasCampaign ? rowData.get(headerIndex.getCampaignIndex()) : null;
            int[] eventCounts = new int[64];
            for (Map.Entry<Integer, Integer> indexToEventNoEntry : headerIndex.getIndexToEventNo().entrySet()) {
                int eventIndex = indexToEventNoEntry.getKey();
                int columnNum = eventIndex + 1;
                int eventNo = indexToEventNoEntry.getValue();
                int eventCount = 0;
                try{
                    eventCount = Integer.parseInt(rowData.get(eventIndex).replace("\"", "").replace(",",""));
                }catch (NumberFormatException ex){
                    log.warn("第" + rowNum + "行，第" + columnNum +"列，存在不可解析数据，跳过！！！！！");
                }
                eventCounts[eventNo] = eventCount;
            }
            if(hasCampaign){
                dayStats.add(DayCampaignStat.builder().campaign(campaign).day(day).otherEventCount(otherEventCount).eventCounts(eventCounts).build());
            }else{
                dayStats.add(DayStat.builder().day(day).otherEventCount(otherEventCount).eventCounts(eventCounts).build());
            }

        }
        return (List<T>) dayStats;
    }


  private static CsvDataHandler.HeaderIndex getHeaderIndex(List<List<String>> csvData, boolean hasCampaign){
        int dayIndex = csvData.get(0).indexOf(GoogleSkanConstant.HEADER_DAY);
        int campaignIndex = csvData.get(0).indexOf(GoogleSkanConstant.HEADER_CAMPAIGN);
        int otherEventIndex = csvData.get(0).indexOf(GoogleSkanConstant.HEADER_OTHER_EVENT);
        if(dayIndex < 0) {
            throw new RuntimeException("未解析到日期表头！");
        }
        if(hasCampaign && campaignIndex < 0) {
            throw new RuntimeException("未解析到Campaign表头！");
        }
        if(otherEventIndex < 0) {
            throw new RuntimeException("未解析到未知事件表头！");
        }
        Map<Integer,Integer> indexToEventNo = new HashMap<>(86);
        List<String> headers = csvData.get(0);
        for (int headerIndex = 0; headerIndex < headers.size(); headerIndex++) {
            String header = headers.get(headerIndex);
            Matcher matcher = eventHeaderPattern.matcher(header);
            if(matcher.matches()){
                indexToEventNo.put(headerIndex, Integer.parseInt(matcher.group(1)));
            }
        }
        if(indexToEventNo.isEmpty()) {
            throw new RuntimeException("未解析到已知事件表头！");
        }
        return CsvDataHandler.HeaderIndex.builder().dayIndex(dayIndex).campaignIndex(campaignIndex).otherEventIndex(otherEventIndex).indexToEventNo(indexToEventNo).build();
    }

    private static boolean checkColumns(int rowNum, int columnSize, CsvDataHandler.HeaderIndex headerIndex, boolean hasCampaign){
        boolean success = true;
        if(headerIndex.getDayIndex() >= columnSize){
            log.warn("行数：" + rowNum + ", 日期不可解析");
            success = false;
        }
        if(hasCampaign && headerIndex.getCampaignIndex() >= columnSize){
            log.warn("行数：" + rowNum + ", Campaign不可解析");
            success = false;
        }
        if(headerIndex.getOtherEventIndex() >= columnSize){
            log.warn("行数：" + rowNum + ", 未知事件不可解析");
            success = false;
        }

        List<Integer> fails = headerIndex.getIndexToEventNo().entrySet().stream().filter(e -> e.getKey() >= columnSize).map(Map.Entry::getValue).collect(Collectors.toList());
        if(!fails.isEmpty()){
            log.warn("行数：" + rowNum + ", 已知事件不可解析，事件id：" + fails);
            success = false;
        }
        return success;
    }
}
