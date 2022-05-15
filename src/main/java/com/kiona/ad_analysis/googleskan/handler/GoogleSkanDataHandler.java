package com.kiona.ad_analysis.googleskan.handler;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.kiona.ad_analysis.googleskan.model.DayCampaignSummary;
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
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;

/**
 * @author yangshuaichao
 * @date 2022/05/10 18:37
 * @description TODO
 */
@Slf4j
public class GoogleSkanDataHandler implements Handler<HttpServerFileUpload> {


    private final List<Future<List<Summary>>> futures = new CopyOnWriteArrayList<>();
    private final HttpServerResponse response;

    public GoogleSkanDataHandler(HttpServerResponse response) {
        this.response = response;
    }


    @Override
    public void handle(HttpServerFileUpload upload) {
        log.info("paramName:{}, fileName:{}, fileSize:{}, fileCharset:{}", upload.name(), upload.filename(), upload.size(), upload.charset());
        Promise<List<Summary>> promise = Promise.promise();
        futures.add(promise.future());
        GoogleSkanFileHandler handler = new GoogleSkanFileHandler(promise);
        upload
            .handler(handler)
            .endHandler(x -> handler.end())
            .exceptionHandler(e -> log.error(e.getMessage()));
    }

    public void end() {
        String excelName = "GoogleSkanResult.xlsx";
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

    private ByteArrayOutputStream getExcelStream(List<List<Summary>> summaries) {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        ExcelWriter excelWriter = EasyExcel.write(outputStream).build();
        try {
            List<DaySummary> daySummaries = getDaySummaries(summaries);
            if (!daySummaries.isEmpty()) {
                WriteSheet writeSheetDay = EasyExcel.writerSheet("按天").head(DaySummary.class).build();
                excelWriter.write(daySummaries, writeSheetDay);
            }

            List<DayCampaignSummary> dayCampaignSummaries = getDayCampaignSummaries(summaries);
            if (!dayCampaignSummaries.isEmpty()) {
                WriteSheet writeSheetDayCampaign = EasyExcel.writerSheet("按Campaign天").head(DayCampaignSummary.class).build();
                excelWriter.write(dayCampaignSummaries, writeSheetDayCampaign);
            }
        } catch (Exception e) {
            log.error("导出excel失败", e);
        } finally {
            excelWriter.finish();
        }
        return outputStream;
    }

    private List<DaySummary> getDaySummaries(List<List<Summary>> summaries) {
        return summaries.stream()
            .flatMap(Collection::stream)
            .filter(s -> s.getClass() == DaySummary.class)
            .map(s -> (DaySummary) s)
            .sorted(Comparator.comparing(DaySummary::getDay))
            .collect(Collectors.toList());
    }

    private List<DayCampaignSummary> getDayCampaignSummaries(List<List<Summary>> summaries) {
        return summaries.stream()
            .flatMap(Collection::stream)
            .filter(s -> s.getClass() == DayCampaignSummary.class)
            .map(s -> (DayCampaignSummary) s)
            .sorted(Comparator.comparing(DayCampaignSummary::getCampaign).thenComparing(DayCampaignSummary::getDay))
            .collect(Collectors.toList());

    }

}
