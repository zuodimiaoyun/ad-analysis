package com.kiona.ad_analysis;

import com.kiona.ad_analysis.googleskan.handler.InstallPurchaseStatHandler;
import com.kiona.ad_analysis.googleskan.handler.TestUploadHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.ext.web.Route;
import io.vertx.ext.web.Router;

public class MainVerticle extends AbstractVerticle {

  @Override
  public void start(Promise<Void> startPromise) {
    HttpServer server = vertx.createHttpServer();
    Router router = Router.router(vertx);
    router.post("/api/v1/googleskan/install-purchase-stat").handler(new InstallPurchaseStatHandler());
    router.post("/upload").blockingHandler(new TestUploadHandler());
    router.route("/c").respond(ctx -> Future.succeededFuture("Hello Docker!"));
    server.requestHandler(router).listen(8888, http -> {
      if (http.succeeded()) {
        startPromise.complete();
        System.out.println("HTTP server started on port 8888");
      } else {
        startPromise.fail(http.cause());
      }
    });
  }

}
