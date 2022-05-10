package com.kiona.ad_analysis.googleskan.handler;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.ExcelWriter;
import com.alibaba.excel.write.metadata.WriteSheet;
import com.kiona.ad_analysis.googleskan.model.DayCampaignStat;
import com.kiona.ad_analysis.googleskan.model.DayCampaignSummary;
import com.kiona.ad_analysis.googleskan.model.DayStat;
import com.kiona.ad_analysis.googleskan.model.DaySummary;
import com.kiona.ad_analysis.googleskan.util.SummaryUtil;
import com.kiona.ad_analysis.util.CsvUtil;
import com.kiona.ad_analysis.googleskan.util.CsvDataParser;
import io.vertx.core.Handler;
import io.vertx.core.MultiMap;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.web.FileUpload;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class InstallPurchaseStatHandler implements Handler<RoutingContext> {

    private static final String dayPath = "";
    private static final String dayCampaignPath = "/Users/yangshuaichao/Downloads/SKan-campaign (3).csv";
    private static final String outPutPath = "结果1.xlsx";
    private static final int startRow = 3;

    public static void main(String[] args) throws IOException {
        List<DaySummary> daySummarys = new ArrayList<>();
        List<DayCampaignSummary> dayCampaignSummarys = new ArrayList<>();
        if(!dayPath.isEmpty()){
            List<DayStat> dayStats = CsvDataParser.parseToDayStat(CsvUtil.read(dayPath, startRow), false);
            daySummarys.addAll(SummaryUtil.getDaySummary(dayStats).stream().sorted(Comparator.comparing(DaySummary::getDay)).collect(Collectors.toList()));

        }
        if(!dayCampaignPath.isEmpty()){
            List<DayCampaignStat> dayCampaignStats = CsvDataParser.parseToDayStat(CsvUtil.read(dayCampaignPath, startRow), true);
            dayCampaignSummarys.addAll(SummaryUtil.getDayCampaignSummary(dayCampaignStats).stream().sorted(Comparator.comparing(DayCampaignSummary::getCampaign).thenComparing(DayCampaignSummary::getDay)).collect(Collectors.toList()));
        }
        exportToExcel(daySummarys, dayCampaignSummarys);
        System.out.println("Done!");
    }

    private static void exportToExcel(List<DaySummary> daySummarys, List<DayCampaignSummary> dayCampaignSummarys) {
        ExcelWriter excelWriter = EasyExcel.write(outPutPath).build();
        try{
            if(!daySummarys.isEmpty()){
                WriteSheet writeSheetDay = EasyExcel.writerSheet("按天").head(DaySummary.class).build();
                excelWriter.write(daySummarys, writeSheetDay);
            }

            if(!dayCampaignSummarys.isEmpty()){
                WriteSheet writeSheetDayCampaign = EasyExcel.writerSheet("按Campaign天").head(DayCampaignSummary.class).build();
                excelWriter.write(dayCampaignSummarys, writeSheetDayCampaign);
            }
        }catch (Exception e){
            log.error("导出excel失败",e);
        }finally {
            excelWriter.finish();
        }

    }

  @Override
  public void handle(RoutingContext ctx) {
    ctx.request().setExpectMultipart(true);
    CsvDataHandler csvDataHandler = new CsvDataHandler(3);
    ctx.request().uploadHandler(csvDataHandler);

  }
}
