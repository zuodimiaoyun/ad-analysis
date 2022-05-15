package com.kiona.ad_analysis.facebookskan.handler;

import com.alibaba.excel.EasyExcel;
import com.kiona.ad_analysis.googleskan.constant.GoogleSkanConstant;
import com.kiona.ad_analysis.googleskan.constant.GoogleSkanPurchaseValue;
import com.kiona.ad_analysis.googleskan.model.DayCampaignSummary;
import com.kiona.ad_analysis.googleskan.model.DaySummary;
import com.kiona.ad_analysis.googleskan.model.Summary;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.parsetools.RecordParser;
import lombok.Builder;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.nio.charset.Charset;
import java.util.*;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * @author yangshuaichao
 * @date 2022/05/15 15:34
 * @description TODO
 */
@Slf4j
public class FacebookSkanFileHandler implements Handler<Buffer> {
    private final int startRow = 1;
    private int currentRow = 0;
    private int currentColumn = 0;
    private Header header;
    private final List<Summary> summaries = new ArrayList<>();

    private final Promise<List<DayCampaignSummary>> donePromise;
    private final Buffer buffer = Buffer.buffer();

    public FacebookSkanFileHandler(Promise<List<DayCampaignSummary>> promise) {
        this.donePromise = promise;
    }

    @Override
    public void handle(Buffer buffer) {
        try {
            this.buffer.appendBuffer(buffer);
        } catch (Exception exception) {
            log.error("解析文件失败", exception);
            donePromise.fail(exception);
        }
    }

    private Handler<String> getLineHandler() {
        return line -> {
            currentRow++;
            if (currentRow < startRow) {
                return;
            }
            List<String> rowData = Arrays.asList(line.split("\t"));

            if (currentRow == startRow) {
                header = parseHeader(rowData);
                return;
            }
            parseData(rowData);
        };
    }

    private Header parseHeader(List<String> headers) {
        int dayIndex = findHeaderIndex(headers, GoogleSkanConstant.HEADER_DAY);
        int campaignIndex = findHeaderIndex(headers, GoogleSkanConstant.HEADER_CAMPAIGN);
        int otherEventIndex = findHeaderIndex(headers, GoogleSkanConstant.HEADER_OTHER_EVENT);

        if (dayIndex < 0) {
            throw new RuntimeException("未解析到日期表头！");
        }
        if (otherEventIndex < 0) {
            throw new RuntimeException("未解析到未知事件表头！");
        }

        Map<Integer, Integer> indexToEventNo = new HashMap<>(86);
        IntStream.range(0, headers.size()).forEach(headerIndex -> {
            String header = headers.get(headerIndex);
            Matcher matcher = GoogleSkanConstant.EVENT_HEADER_PATTERN.matcher(header);
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

    private int findHeaderIndex(List<String> headers, String headerPattern) {
        return IntStream.range(0, headers.size()).filter(index -> headers.get(index).matches(headerPattern)).findFirst().orElse(-1);
    }

    private void parseData(List<String> rowData) {
        currentColumn++;
        if (!checkColumns(currentColumn, rowData.size(), header)) {

            log.warn("第" + currentColumn + "行，存在不可解析数据，跳过！！！！！");
            return;
        }

        String day = rowData.get(header.getDayIndex());
        int otherEventCount = Integer.parseInt(rowData.get(header.getOtherEventIndex()));
        String campaign = header.getCampaignIndex() >= 0 ? rowData.get(header.getCampaignIndex()) : null;
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

    private boolean checkColumns(int rowNum, int columnSize, Header headerIndex) {
        boolean success = true;
        if (headerIndex.getDayIndex() >= columnSize) {
            log.warn("行数：" + rowNum + ", 日期不可解析");
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

    public void end() {
        List<DayCampaignSummary> summaryList = EasyExcel.read(new ByteArrayInputStream(buffer.getBytes())).head(DayCampaignSummary.class).doReadAllSync();
        summaryList.removeIf(s -> s.getDay() == null || s.getDay().isEmpty());
        donePromise.complete(summaryList);
    }

    @Data
    @Builder
    public static class Header {
        private int dayIndex;
        private int campaignIndex;
        private int otherEventIndex;
        private Map<Integer, Integer> indexToEventNo;
    }

    /**
     * 有中文解析乱码问题，暂时弃用
     */
    private RecordParser getCsvHandler() {
        return RecordParser.newDelimited("\n", buffer -> getLineHandler().handle(buffer.toString(Charset.forName("unicode"))));
    }
}
