package com.kiona.ad_analysis.googleskan.handler;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class GoogleSkanStatHandler implements Handler<RoutingContext> {

    @Override
    public void handle(RoutingContext ctx) {
        ctx.request().setExpectMultipart(true);
        GoogleSkanDataHandler dataHandler = new GoogleSkanDataHandler(ctx.response());
        ctx.request()
            .uploadHandler(dataHandler)
            .endHandler(x -> dataHandler.end());
    }


}
