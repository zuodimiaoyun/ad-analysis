package com.kiona.ad_analysis;

import io.vertx.core.Vertx;

/**
 * @author yangshuaichao
 * @date 2022/05/10 19:54
 * @description TODO
 */
public class Main {
  public static void main(String[] args) {
    Vertx vertx = Vertx.vertx();
    vertx.deployVerticle(MainVerticle.class.getName());
  }
}
