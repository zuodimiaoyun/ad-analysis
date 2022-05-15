package com.kiona.ad_analysis.facebookskan.handler;

import io.vertx.core.Handler;
import io.vertx.ext.web.RoutingContext;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FacebookStatHandler implements Handler<RoutingContext> {

    @Override
    public void handle(RoutingContext ctx) {
        ctx.request().setExpectMultipart(true);
        FacebookDataHandler dataHandler = new FacebookDataHandler(ctx.response());
        ctx.request()
            .uploadHandler(dataHandler)
            .endHandler(x -> dataHandler.end())
            .exceptionHandler(x -> ctx.response().setStatusCode(500).setStatusMessage("failed").end("failed"))
        ;
    }


}
