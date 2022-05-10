package com.kiona.ad_analysis.googleskan.handler;

import io.vertx.core.Handler;
import io.vertx.core.parsetools.RecordParser;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * @author yangshuaichao
 * @date 2022/05/10 19:30
 * @description TODO
 */
@Slf4j
public class TestUploadHandler implements Handler<RoutingContext> {
  private List<String> lines = new ArrayList<>();
  @Override
  public void handle(RoutingContext ctx) {
    ctx.request().setExpectMultipart(true);
    ctx.request().uploadHandler(fileUpload -> {
      log.info(fileUpload.name());
      log.info(fileUpload.filename());
      fileUpload.handler(RecordParser.newDelimited("\n", lineBuffer -> {
          String line = lineBuffer.toString();
          lines.add(line);
          log.info(line);
        })
      ).endHandler(x -> ctx.response().end(lines.toString()));
    });
  }

  public List<String> getLines() {
    return lines;
  }
}
