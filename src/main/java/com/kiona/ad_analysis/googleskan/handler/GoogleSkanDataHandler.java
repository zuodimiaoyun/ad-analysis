package com.kiona.ad_analysis.googleskan.handler;

import com.kiona.ad_analysis.googleskan.constant.GoogleSkanConstant;
import com.kiona.ad_analysis.googleskan.constant.GoogleSkanPurchaseValue;
import com.kiona.ad_analysis.googleskan.model.DayCampaignSummary;
import com.kiona.ad_analysis.googleskan.model.DaySummary;
import com.kiona.ad_analysis.googleskan.model.Summary;
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
import java.util.stream.IntStream;

/**
 * @author yangshuaichao
 * @date 2022/05/10 18:37
 * @description TODO
 */
@Slf4j
public class GoogleSkanDataHandler implements Handler<HttpServerFileUpload> {
    private static final Pattern EVENT_HEADER_PATTERN = Pattern.compile("(\\d+)_SKAdNetwork Conversions");

    private final int startRow = 3;

    private int currentRow = 0;
    private int currentColumn = 0;
    private boolean hasCampaign = false;
    private Header header;
    private final List<Summary> summaries = new ArrayList<>();

    @Override
    public void handle(HttpServerFileUpload httpServerFileUpload) {
        reset();
        hasCampaign = "campaignFile".equals(httpServerFileUpload.name());
        httpServerFileUpload.handler(RecordParser.newDelimited("\n", buffer -> {
            currentRow++;
            if (currentRow < startRow) {
                return;
            }
            List<String> rowData = Arrays.asList(new String(buffer.getBytes(), Charset.forName("unicode")).split("\t"));

            if (currentRow == startRow) {
                header = parseHeader(rowData);
                return;
            }
            if (currentRow > startRow) {
                parseData(rowData, hasCampaign);
            }
        }));
    }

    private void reset() {
        this.currentColumn = 0;
        this.currentRow = 0;
        this.hasCampaign = false;
        this.header = null;
    }

    private Header parseHeader(List<String> headers) {
        int dayIndex = headers.indexOf(GoogleSkanConstant.HEADER_DAY);
        int campaignIndex = headers.indexOf(GoogleSkanConstant.HEADER_CAMPAIGN);
        int otherEventIndex = headers.indexOf(GoogleSkanConstant.HEADER_OTHER_EVENT);

        if (dayIndex < 0) {
            throw new RuntimeException("未解析到日期表头！");
        }
        if (hasCampaign && campaignIndex < 0) {
            throw new RuntimeException("未解析到Campaign表头！");
        }
        if (otherEventIndex < 0) {
            throw new RuntimeException("未解析到未知事件表头！");
        }

        Map<Integer, Integer> indexToEventNo = new HashMap<>(86);
        IntStream.range(0, headers.size()).forEach(headerIndex -> {
            String header = headers.get(headerIndex);
            Matcher matcher = EVENT_HEADER_PATTERN.matcher(header);
            if (matcher.matches()) {
                indexToEventNo.put(headerIndex, Integer.parseInt(matcher.group(1)));
            }
        });

        if (indexToEventNo.isEmpty()) {
            throw new RuntimeException("未解析到已知事件表头！");
        }

        return Header.builder()
            .dayIndex(dayIndex)
            .campaignIndex(campaignIndex)
            .otherEventIndex(otherEventIndex)
            .indexToEventNo(indexToEventNo)
            .build();
    }

    public void parseData(List<String> rowData, boolean hasCampaign) {
        currentColumn++;
        if (!checkColumns(currentColumn, rowData.size(), header, hasCampaign)) {
            log.warn("第" + currentColumn + "行，存在不可解析数据，跳过！！！！！");
            return;
        }

        String day = rowData.get(header.getDayIndex());
        int otherEventCount = Integer.parseInt(rowData.get(header.getOtherEventIndex()));
        String campaign = hasCampaign ? rowData.get(header.getCampaignIndex()) : null;
        int[] eventCounts = parseEventCounts(rowData);

        summaries.add(summary(day, campaign, otherEventCount, eventCounts));
    }

    private int[] parseEventCounts(List<String> rowData) {
        int[] eventCounts = new int[64];
        for (Map.Entry<Integer, Integer> indexToEventNoEntry : header.getIndexToEventNo().entrySet()) {
            int eventIndex = indexToEventNoEntry.getKey();
            int columnNum = eventIndex + 1;
            int eventNo = indexToEventNoEntry.getValue();
            int eventCount = 0;
            try {
                eventCount = Integer.parseInt(rowData.get(eventIndex).replace("\"", "").replace(",", ""));
            } catch (NumberFormatException ex) {
                log.warn("第" + currentColumn + "行，第" + columnNum + "列，存在不可解析数据，跳过！！！！！");
            }
            eventCounts[eventNo] = eventCount;
        }
        return eventCounts;
    }

    private Summary summary(String day, String campaign, int otherEventCount, int[] eventCounts) {
        int install = otherEventCount + Arrays.stream(eventCounts).sum();
        int purchase = getPurchaseCount(day, eventCounts);
        double purchaseValue = getPurchaseValue(day, eventCounts);
        Summary summary;
        if (campaign != null) {
            summary = DayCampaignSummary.builder().day(day).campaign(campaign).build();
        } else {
            summary = DaySummary.builder().day(day).build();
        }
        summary.setInstall(install);
        summary.setPurchase(purchase);
        summary.setPurchaseValue(purchaseValue);
        return summary;
    }

    private int getPurchaseCount(String day, int[] eventCounts) {
        return IntStream.range(0, eventCounts.length)
            .filter(eventNo -> GoogleSkanPurchaseValue.isPurchaseEvent(eventNo, day))
            .map(eventNo -> eventCounts[eventNo])
            .sum();
    }

    private static double getPurchaseValue(String day, int[] eventCounts) {
        return IntStream.range(0, eventCounts.length)
            .filter(eventNo -> GoogleSkanPurchaseValue.isPurchaseEvent(eventNo, day))
            .mapToDouble(eventNo -> GoogleSkanPurchaseValue.getValue(eventNo, day) * eventCounts[eventNo])
            .sum();
    }

    private boolean checkColumns(int rowNum, int columnSize, Header headerIndex, boolean hasCampaign) {
        boolean success = true;
        if (headerIndex.getDayIndex() >= columnSize) {
            log.warn("行数：" + rowNum + ", 日期不可解析");
            success = false;
        }
        if (hasCampaign && headerIndex.getCampaignIndex() >= columnSize) {
            log.warn("行数：" + rowNum + ", Campaign不可解析");
            success = false;
        }
        if (headerIndex.getOtherEventIndex() >= columnSize) {
            log.warn("行数：" + rowNum + ", 未知事件不可解析");
            success = false;
        }

        List<Integer> fails = headerIndex.getIndexToEventNo().entrySet().stream().filter(e -> e.getKey() >= columnSize).map(Map.Entry::getValue).collect(Collectors.toList());
        if (!fails.isEmpty()) {
            log.warn("行数：" + rowNum + ", 已知事件不可解析，事件id：" + fails);
            success = false;
        }
        return success;
    }

    @Data
    @Builder
    public static class Header {
        private int dayIndex;
        private int campaignIndex;
        private int otherEventIndex;
        private Map<Integer, Integer> indexToEventNo;
    }

    public List<Summary> getSummaries() {
        return summaries;
    }

    public int getCurrentRow() {
        return currentRow;
    }
}
