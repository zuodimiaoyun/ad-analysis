package com.kiona.ad_analysis.googleskan.handler;

import com.kiona.ad_analysis.googleskan.constant.GoogleSkanConstant;
import com.kiona.ad_analysis.googleskan.model.DayCampaignStat;
import com.kiona.ad_analysis.googleskan.model.DayStat;
import com.kiona.ad_analysis.googleskan.model.Stat;
import com.kiona.ad_analysis.googleskan.util.CsvDataParser;
import io.vertx.core.Handler;
import io.vertx.core.http.HttpServerFileUpload;
import io.vertx.core.parsetools.RecordParser;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author yangshuaichao
 * @date 2022/05/10 18:37
 * @description TODO
 */
@Slf4j
public class CsvDataHandler implements Handler<HttpServerFileUpload> {
  private static final Pattern EVENT_HEADER_PATTERN = Pattern.compile("(\\d+)_SKAdNetwork Conversions");

  private final int startRow;

  private int currentRow = 0;
  private int currentColumn = 0;
  private boolean hasCampaign = false;
  private HeaderIndex headerIndex;
  private List<Stat> stats = new ArrayList<>();

  public CsvDataHandler() {
    this(1);
  }

  public CsvDataHandler(int startRow) {
    this.startRow = startRow;
  }


  @Override
  public void handle(HttpServerFileUpload httpServerFileUpload) {
    hasCampaign = "campaignFile".equals(httpServerFileUpload.name());
    httpServerFileUpload.handler(RecordParser.newDelimited("\n", buffer -> {
      currentRow ++;
      if(currentRow < startRow){
        return;
      }
      List<String> rowData = Arrays.asList(new String(buffer.getBytes(), Charset.forName("unicode")).split("\t"));
      if(currentRow == startRow){
        headerIndex = getHeaderIndex(rowData);
        return;
      }
      if(currentRow > startRow){
        parseToDayStat(rowData, hasCampaign);
      }
    }));
  }

  private HeaderIndex getHeaderIndex(List<String> headers){
    int dayIndex = headers.indexOf(GoogleSkanConstant.HEADER_DAY);
    int campaignIndex = headers.indexOf(GoogleSkanConstant.HEADER_CAMPAIGN);
    int otherEventIndex = headers.indexOf(GoogleSkanConstant.HEADER_OTHER_EVENT);
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
    for (int headerIndex = 0; headerIndex < headers.size(); headerIndex++) {
      String header = headers.get(headerIndex);
      Matcher matcher = EVENT_HEADER_PATTERN.matcher(header);
      if(matcher.matches()){
        indexToEventNo.put(headerIndex, Integer.parseInt(matcher.group(1)));
      }
    }
    if(indexToEventNo.isEmpty()) {
      throw new RuntimeException("未解析到已知事件表头！");
    }
    return HeaderIndex.builder().dayIndex(dayIndex).campaignIndex(campaignIndex).otherEventIndex(otherEventIndex).indexToEventNo(indexToEventNo).build();
  }

  public void parseToDayStat(List<String> rowData, boolean hasCampaign){
      currentColumn ++;
      int columnSize = rowData.size();
      boolean checkSuccess = checkColumns(currentColumn, columnSize, headerIndex, hasCampaign);
      if(!checkSuccess){
        log.warn("第" + currentColumn + "行，存在不可解析数据，跳过！！！！！");
        return ;
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
          log.warn("第" + currentColumn + "行，第" + columnNum +"列，存在不可解析数据，跳过！！！！！");
        }
        eventCounts[eventNo] = eventCount;
      }
      if(hasCampaign){
        stats.add(DayCampaignStat.builder().campaign(campaign).day(day).otherEventCount(otherEventCount).eventCounts(eventCounts).build());
      }else{
        stats.add(DayStat.builder().day(day).otherEventCount(otherEventCount).eventCounts(eventCounts).build());
      }
  }


  private boolean checkColumns(int rowNum, int columnSize, CsvDataHandler.HeaderIndex headerIndex, boolean hasCampaign){
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

  @Data
  @Builder
  public static class HeaderIndex{
      private int dayIndex;
      private int campaignIndex;
      private int otherEventIndex;
      private Map<Integer, Integer> indexToEventNo;
  }

  public List<Stat> getStats() {
    return stats;
  }
}
