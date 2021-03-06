package com.kiona.ad_analysis.facebookskan.handler;

import com.alibaba.excel.EasyExcel;
import com.kiona.ad_analysis.googleskan.model.DayCampaignSummary;
import com.kiona.ad_analysis.googleskan.model.DayCreativeSummary;
import com.kiona.ad_analysis.googleskan.model.TimeSummary;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.util.*;

/**
 * @author yangshuaichao
 * @date 2022/05/15 15:34
 * @description TODO
 */
@Slf4j
public class FacebookSkanFileHandler implements Handler<Buffer> {

    private final Promise<List<? extends TimeSummary>> donePromise;
    private final Buffer buffer = Buffer.buffer();

    public FacebookSkanFileHandler(Promise<List<? extends TimeSummary>> promise) {
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


    public void end() {
        List<? extends TimeSummary> summaries;
        List<DayCreativeSummary> summaryList1 = EasyExcel.read(new ByteArrayInputStream(buffer.getBytes())).head(DayCreativeSummary.class).doReadAllSync();
        if(!summaryList1.isEmpty() && summaryList1.get(0).getCreative() != null){
            summaries = summaryList1;
        }else{
            summaries = EasyExcel.read(new ByteArrayInputStream(buffer.getBytes())).head(DayCampaignSummary.class).doReadAllSync();

        }
        summaries.removeIf(s -> s.getDay() == null || s.getDay().isEmpty());
        donePromise.complete(summaries);
    }
}
