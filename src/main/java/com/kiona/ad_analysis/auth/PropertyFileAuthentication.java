package com.kiona.ad_analysis.auth;

import com.kiona.ad_analysis.auth.impl.PropertyFileAuthenticationImpl;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.authentication.AuthenticationProvider;

/**
 * @author yangshuaichao
 * @date 2022/05/13 23:18
 * @description TODO
 */
public interface PropertyFileAuthentication extends AuthenticationProvider {
    static PropertyFileAuthentication create(Vertx vertx, String path) {
        return new PropertyFileAuthenticationImpl(vertx, path);
    }
}
