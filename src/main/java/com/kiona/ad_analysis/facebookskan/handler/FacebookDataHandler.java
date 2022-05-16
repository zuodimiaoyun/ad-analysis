package com.kiona.ad_analysis.facebookskan.handler;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.alibaba.excel.write.style.column.LongestMatchColumnWidthStyleStrategy;
import com.kiona.ad_analysis.googleskan.constant.FacebookSkanConstant;
import com.kiona.ad_analysis.googleskan.model.DayCampaignSummary;
import com.kiona.ad_analysis.googleskan.model.DayCreativeSummary;
import com.kiona.ad_analysis.googleskan.model.DaySummary;
import com.kiona.ad_analysis.googleskan.model.Summary;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.HttpServerFileUpload;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.http.impl.MimeMapping;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.stream.Collectors;

/**
 * @author yangshuaichao
 * @date 2022/05/10 18:37
 * @description TODO
 */
@Slf4j
public class FacebookDataHandler implements Handler<HttpServerFileUpload> {
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
    private final List<Future<List<? extends DaySummary>>> futures = new CopyOnWriteArrayList<>();
    private final HttpServerResponse response;

    public FacebookDataHandler(HttpServerResponse response) {
        this.response = response;
    }


    @Override
    public void handle(HttpServerFileUpload upload) {

        log.info("paramName:{}, fileName:{}, fileSize:{}, fileCharset:{}", upload.name(), upload.filename(), upload.size(), upload.charset());
        Promise<List<? extends DaySummary>> promise = Promise.promise();
        futures.add(promise.future());
        FacebookSkanFileHandler handler = new FacebookSkanFileHandler(promise);
        upload
            .handler(handler)
            .endHandler(x -> handler.end())
            .exceptionHandler(e -> log.error(e.getMessage()));
    }

    public void end() {
        String excelName = "FacebookSkanResult.xlsx";
        CompositeFuture.all(Collections.unmodifiableList(futures)).onComplete(ar -> {
            if (ar.succeeded()) {
                response
                    .putHeader(HttpHeaders.CONTENT_TYPE, MimeMapping.getMimeTypeForFilename(excelName))
                    .putHeader("Content-Disposition", "attachment; filename=\"" + excelName + "\"")
                    .end(Buffer.buffer(getExcelStream(ar.result().list()).toByteArray()));
            } else {
                log.error("读取数据失败");
                response.end("读取数据失败！");
            }
        });
    }

    private ByteArrayOutputStream getExcelStream(List<List<? extends DaySummary>> summaries) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ExcelWriter excelWriter = EasyExcel.write(outputStream).registerWriteHandler(new LongestMatchColumnWidthStyleStrategy()).build();
        List<? extends DaySummary> data = summaries.stream().flatMap(Collection::stream).collect(Collectors.toList());
        try {
            if(data.isEmpty()){
                outputStream.write("失败".getBytes(StandardCharsets.UTF_8));
                return outputStream;
            }
            if(data.get(0) instanceof DayCampaignSummary){
                List<DayCampaignSummary> list = data.stream().map(d -> (DayCampaignSummary) d).collect(Collectors.toList());
                List<DaySummary> daySummaries = getDaySummaries(list);
                if (!daySummaries.isEmpty()) {
                    WriteSheet writeSheetDay = EasyExcel.writerSheet("按天").head(DaySummary.class).build();
                    excelWriter.write(daySummaries, writeSheetDay);
                }

                List<DayCampaignSummary> dayCampaignSummaries = getDayCampaignSummaries(list);
                if (!dayCampaignSummaries.isEmpty()) {
                    WriteSheet writeSheetDayCampaign = EasyExcel.writerSheet("按Campaign天").head(DayCampaignSummary.class).build();
                    excelWriter.write(dayCampaignSummaries, writeSheetDayCampaign);
                }
            }else if(data.get(0) instanceof DayCreativeSummary){
                List<DayCreativeSummary> list = data.stream().map(d -> (DayCreativeSummary) d).peek(d -> {
                    Matcher matcher = FacebookSkanConstant.creativePattern.matcher(d.getCreative());
                    if(matcher.matches()){
                        d.setCreative(matcher.group(1));
                    }
                }).collect(Collectors.toList());
                List<DayCreativeSummary> dayCreativeSummaries = getDayCreativeSummaries(list);
                if (!dayCreativeSummaries.isEmpty()) {
                    WriteSheet writeSheetDayCreative = EasyExcel.writerSheet("按素材天").head(DayCreativeSummary.class).build();
                    excelWriter.write(dayCreativeSummaries, writeSheetDayCreative);
                }
            }
        } catch (Exception e) {
            log.error("导出excel失败", e);
        } finally {
            excelWriter.finish();
        }
        return outputStream;
    }


    private List<DaySummary> getDaySummaries(List<DayCampaignSummary> summaries) {
        return summaries
            .stream()
            .map(DaySummary::getDay)
            .distinct()
            .map(day -> getSummaryByDay(day, summaries))
            .sorted(Comparator.comparing(DaySummary::getDay))
            .collect(Collectors.toList());
    }

    private List<DayCampaignSummary> getDayCampaignSummaries(List<DayCampaignSummary> summaries) {
        return summaries
            .stream()
            .map(s -> DayCampaignSummary.builder().campaign(s.getCampaign()).day(s.getDay()).build())
            .distinct()
            .map(d -> getSummaryByDayAndCampaign(d.getDay(), d.getCampaign(), summaries))
            .sorted(Comparator.comparing(DayCampaignSummary::getCampaign).thenComparing(DayCampaignSummary::getDay))
            .collect(Collectors.toList());

    }

    private List<DayCreativeSummary> getDayCreativeSummaries(List<DayCreativeSummary> summaries) {
        return summaries
            .stream()
            .map(s -> DayCreativeSummary.builder().creative(s.getCreative()).day(s.getDay()).build())
            .distinct()
            .map(d -> getSummaryByDayAndCreative(d.getDay(), d.getCreative(), summaries))
            .sorted(Comparator.comparing(DayCreativeSummary::getCreative).thenComparing(DayCreativeSummary::getDay))
            .collect(Collectors.toList());

    }

    @SneakyThrows
    private DaySummary getSummaryByDay(String day, List<? extends DaySummary> summaries) {
        Date summaryDay = dateFormat.parse(day);
        Date installDay = Date.from(summaryDay.toInstant().plus(1, ChronoUnit.DAYS));
        Date purchaseDay = Date.from(summaryDay.toInstant().plus(2, ChronoUnit.DAYS));
        return DaySummary.builder()
            .day(day)
            .costMoney(summaries.stream().filter(s -> Objects.equals(s.getDay(), day)).mapToDouble(Summary::getCostMoney).sum())
            .install(summaries.stream().filter(s -> Objects.equals(s.getDay(), dateFormat.format(installDay))).mapToInt(Summary::getInstall).sum())
            .purchase(summaries.stream().filter(s -> Objects.equals(s.getDay(), dateFormat.format(purchaseDay))).mapToInt(Summary::getPurchase).sum())
            .purchaseValue(summaries.stream().filter(s -> Objects.equals(s.getDay(), dateFormat.format(purchaseDay))).mapToDouble(Summary::getPurchaseValue).sum())
            .build();
    }

    @SneakyThrows
    private DayCampaignSummary getSummaryByDayAndCampaign(String day, String campaign, List<? extends DayCampaignSummary> summaries) {
        Date summaryDay = dateFormat.parse(day);
        Date installDay = Date.from(summaryDay.toInstant().plus(1, ChronoUnit.DAYS));
        Date purchaseDay = Date.from(summaryDay.toInstant().plus(2, ChronoUnit.DAYS));
        return DayCampaignSummary.builder()
            .day(day)
            .campaign(campaign)
            .costMoney(summaries.stream().filter(s -> Objects.equals(s.getDay(), day) && Objects.equals(s.getCampaign(), campaign)).mapToDouble(Summary::getCostMoney).sum())
            .install(summaries.stream().filter(s -> Objects.equals(s.getDay(), dateFormat.format(installDay)) && Objects.equals(s.getCampaign(), campaign)).mapToInt(Summary::getInstall).sum())
            .purchase(summaries.stream().filter(s -> Objects.equals(s.getDay(), dateFormat.format(purchaseDay)) && Objects.equals(s.getCampaign(), campaign)).mapToInt(Summary::getPurchase).sum())
            .purchaseValue(summaries.stream().filter(s -> Objects.equals(s.getDay(), dateFormat.format(purchaseDay)) && Objects.equals(s.getCampaign(), campaign)).mapToDouble(Summary::getPurchaseValue).sum())
            .build();
    }


    @SneakyThrows
    private DayCreativeSummary getSummaryByDayAndCreative(String day, String creative, List<? extends DayCreativeSummary> summaries) {
        Date summaryDay = dateFormat.parse(day);
        Date installDay = Date.from(summaryDay.toInstant().plus(1, ChronoUnit.DAYS));
        Date purchaseDay = Date.from(summaryDay.toInstant().plus(2, ChronoUnit.DAYS));
        return DayCreativeSummary.builder()
            .day(day)
            .creative(creative)
            .click(summaries.stream().filter(s -> Objects.equals(s.getDay(), day) && Objects.equals(s.getCreative(), creative)).mapToInt(DayCreativeSummary::getClick).sum())
            .display(summaries.stream().filter(s -> Objects.equals(s.getDay(), day) && Objects.equals(s.getCreative(), creative)).mapToInt(DayCreativeSummary::getDisplay).sum())
            .costMoney(summaries.stream().filter(s -> Objects.equals(s.getDay(), day) && Objects.equals(s.getCreative(), creative)).mapToDouble(Summary::getCostMoney).sum())
            .install(summaries.stream().filter(s -> Objects.equals(s.getDay(), dateFormat.format(installDay)) && Objects.equals(s.getCreative(), creative)).mapToInt(Summary::getInstall).sum())
            .purchase(summaries.stream().filter(s -> Objects.equals(s.getDay(), dateFormat.format(purchaseDay)) && Objects.equals(s.getCreative(), creative)).mapToInt(Summary::getPurchase).sum())
            .purchaseValue(summaries.stream().filter(s -> Objects.equals(s.getDay(), dateFormat.format(purchaseDay)) && Objects.equals(s.getCreative(), creative)).mapToDouble(Summary::getPurchaseValue).sum())
            .build();
    }
}
