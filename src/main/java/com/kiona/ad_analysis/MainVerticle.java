package com.kiona.ad_analysis;

import com.kiona.ad_analysis.auth.PropertyFileAuthentication;
import com.kiona.ad_analysis.facebookskan.handler.FacebookStatHandler;
import com.kiona.ad_analysis.googleskan.handler.GoogleSkanStatHandler;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.http.HttpServer;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.AuthenticationHandler;
import io.vertx.ext.web.handler.FormLoginHandler;
import io.vertx.ext.web.handler.SessionHandler;
import io.vertx.ext.web.handler.StaticHandler;
import io.vertx.ext.web.sstore.LocalSessionStore;


public class MainVerticle extends AbstractVerticle {

    @Override
    public void start(Promise<Void> startPromise) {
        HttpServer server = vertx.createHttpServer();
        Router router = Router.router(vertx);
//        router.route().handler(getSessionHandler());
//        router.post("/login").handler(getAuthHandler());
        router.post("/api/v1/facebookskan/analysis").handler(new FacebookStatHandler());
        router.post("/api/v1/googleskan/analysis").handler(new GoogleSkanStatHandler());
        router.route("/c").respond(ctx -> Future.succeededFuture("Hello Docker!"));
        router.route("/css/*").handler(StaticHandler.create());
        router.get("/").handler(x -> x.response()
            .putHeader("Content-Type", "text/html")
            .sendFile("html/index.html")
        );
        server.requestHandler(router).listen(8888, http -> {
            if (http.succeeded()) {
                startPromise.complete();
                System.out.println("HTTP server started on port 8888");
            } else {
                startPromise.fail(http.cause());
            }
        });
    }

    private SessionHandler getSessionHandler() {
        return SessionHandler.create(LocalSessionStore.create(vertx)).setSessionTimeout(10080000L);
    }

    private AuthenticationHandler getAuthHandler() {
        return (AuthenticationHandler) FormLoginHandler.create(PropertyFileAuthentication.create(vertx, "user.properties"));
    }
}
