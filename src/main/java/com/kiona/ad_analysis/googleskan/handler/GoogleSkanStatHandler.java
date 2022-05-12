package com.kiona.ad_analysis.googleskan.handler;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.kiona.ad_analysis.googleskan.model.DayCampaignSummary;
import com.kiona.ad_analysis.googleskan.model.DaySummary;
import com.kiona.ad_analysis.googleskan.model.Summary;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.http.impl.MimeMapping;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
public class GoogleSkanStatHandler implements Handler<RoutingContext> {

    @Override
    public void handle(RoutingContext ctx) {
        ctx.request().setExpectMultipart(true);
        String excelName = "GoogleSkanResult.xlsx";
        GoogleSkanDataHandler dataHandler = new GoogleSkanDataHandler();
        ctx.request()
            .uploadHandler(dataHandler)
            .endHandler(x -> {
                    if (dataHandler.getSummaries().isEmpty()) {
                        ctx.response()
                            .end("文件未上传！", "GBK");
                    } else {
                        ctx.response()
                            .putHeader(HttpHeaders.CONTENT_TYPE, MimeMapping.getMimeTypeForFilename(excelName))
                            .putHeader("Content-Disposition", "attachment; filename=\"" + excelName + "\"")
                            .end(Buffer.buffer(getExcelStream(dataHandler.getSummaries()).toByteArray()));
                    }
                }

            )
        ;
    }


    private ByteArrayOutputStream getExcelStream(List<Summary> summaries) {
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

    private List<DaySummary> getDaySummaries(List<Summary> summaries) {
        return summaries.stream()
            .filter(s -> s.getClass() == DaySummary.class)
            .map(s -> (DaySummary) s)
            .sorted(Comparator.comparing(DaySummary::getDay))
            .collect(Collectors.toList());
    }

    private List<DayCampaignSummary> getDayCampaignSummaries(List<Summary> summaries) {
        return summaries.stream()
            .filter(s -> s.getClass() == DayCampaignSummary.class)
            .map(s -> (DayCampaignSummary) s)
            .sorted(Comparator.comparing(DayCampaignSummary::getCampaign).thenComparing(DayCampaignSummary::getDay))
            .collect(Collectors.toList());

    }
}
