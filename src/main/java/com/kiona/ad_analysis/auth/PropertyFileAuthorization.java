package com.kiona.ad_analysis.auth;

import com.kiona.ad_analysis.auth.impl.PropertyFileAuthenticationImpl;
import io.vertx.core.Vertx;
import io.vertx.ext.auth.authorization.AuthorizationProvider;

/**
 * @author yangshuaichao
 * @date 2022/05/13 23:29
 * @description TODO
 */
public interface PropertyFileAuthorization extends AuthorizationProvider {
    static PropertyFileAuthorization create(Vertx vertx, String path) {
        return new PropertyFileAuthenticationImpl(vertx, path);
    }
}
