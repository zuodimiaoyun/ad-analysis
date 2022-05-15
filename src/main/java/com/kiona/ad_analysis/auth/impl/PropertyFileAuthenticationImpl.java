package com.kiona.ad_analysis.auth.impl;

import com.kiona.ad_analysis.auth.PropertyFileAuthentication;
import com.kiona.ad_analysis.auth.PropertyFileAuthorization;
import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.authentication.Credentials;
import io.vertx.ext.auth.authentication.UsernamePasswordCredentials;
import io.vertx.ext.auth.authorization.Authorization;
import io.vertx.ext.auth.authorization.RoleBasedAuthorization;
import io.vertx.ext.auth.authorization.WildcardPermissionBasedAuthorization;
import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author yangshuaichao
 * @date 2022/05/13 23:21
 * @description TODO
 */
@Slf4j
public class PropertyFileAuthenticationImpl implements PropertyFileAuthentication, PropertyFileAuthorization {
    private final static Logger logger = Logger.getLogger(PropertyFileAuthentication.class.getName());
    private final Vertx vertx;
    private final String path;

    private Map<String, User> users;
    private Map<String, Role> roles;

    public PropertyFileAuthenticationImpl(Vertx vertx, String path) {
        this.vertx = Objects.requireNonNull(vertx);
        this.path = Objects.requireNonNull(path);
    }

    private void readFile(Handler<AsyncResult<Boolean>> handler) {
        vertx.fileSystem().readFile(path, readResponse -> {
            this.users = new HashMap<>();
            this.roles = new HashMap<>();
            if (readResponse.failed()) {
                handler.handle(Future.failedFuture(readResponse.cause()));
            } else {
                String fileContent = readResponse.result().toString(StandardCharsets.UTF_8);
                String[] lines = fileContent.split("\n");
                for (String line : lines) {
                    if (line.length() == 0 || line.startsWith("#")) {
                        // skip empty lines or comments
                        continue;
                    }

                    if (line.startsWith("user.")) {
                        logger.log(Level.FINE, () -> "read user line: " + line);
                        String usernameAndRoles = line.substring(5);
                        int index = usernameAndRoles.indexOf('=');
                        String tmpName = index > 0 ? usernameAndRoles.substring(0, index).trim() : "";
                        String tmpRoles = index > 0 ? usernameAndRoles.substring(index + 1).trim() : "";
                        if (tmpName.length() > 0) {
                            User user = new User(tmpName);
                            users.put(tmpName, user);
                            int roleIndex = 0;
                            for (String tmpRole : tmpRoles.split(",")) {
                                tmpRole = tmpRole.trim();
                                if (roleIndex == 0) {
                                    user.password = tmpRole;
                                } else if (tmpRole.length() > 0) {
                                    Role role = roles.get(tmpRole);
                                    if (role == null) {
                                        role = new Role(tmpRole);
                                        roles.put(tmpRole, role);
                                    }
                                    user.addRole(role);
                                }
                                roleIndex++;
                            }
                        } else {
                            logger.log(Level.WARNING, () -> "read blank username - " + line);
                        }
                    } else if (line.startsWith("role.")) {
                        logger.log(Level.FINE, () -> "read role line - " + line);
                        String roleAndProperties = line.substring(5);
                        int index = roleAndProperties.indexOf('=');
                        String tmpName = index > 0 ? roleAndProperties.substring(0, index).trim() : "";
                        String tmpProperties = index > 0 ? roleAndProperties.substring(index + 1).trim() : "";
                        if (tmpName.length() > 0) {
                            Role role = roles.get(tmpName);
                            if (role == null) {
                                role = new Role(tmpName);
                                roles.put(tmpName, role);
                            }
                            for (String tmpProperty : tmpProperties.split(",")) {
                                tmpProperty = tmpProperty.trim();
                                if (tmpProperty.length() > 0) {
                                    role.addPermission(tmpProperty);
                                }
                            }
                        } else {
                            logger.log(Level.WARNING, () -> "read blank role - " + line);
                        }
                    } else {
                        logger.log(Level.WARNING, () -> "read unknow line - " + line);
                    }
                }
                handler.handle(Future.succeededFuture(Boolean.TRUE));
            }
        });
    }

    private void getUser(String username, Handler<AsyncResult<User>> handler) {
        if (users == null) {
            readFile(readFileResponse -> {
                User result = users.get(username);
                handler.handle(result != null ? Future.succeededFuture(result) : Future.failedFuture("unknown user"));
            });
        } else {
            User result = users.get(username);
            handler.handle(result != null ? Future.succeededFuture(result) : Future.failedFuture("unknown user"));
        }
    }

    @Override
    public void authenticate(JsonObject authInfo, Handler<AsyncResult<io.vertx.ext.auth.User>> resultHandler) {
        authenticate(new UsernamePasswordCredentials(authInfo), resultHandler);
    }

    @Override
    public void authenticate(Credentials credentials, Handler<AsyncResult<io.vertx.ext.auth.User>> resultHandler) {
        try {
            UsernamePasswordCredentials authInfo = (UsernamePasswordCredentials) credentials;
            authInfo.checkValid(null);

            getUser(authInfo.getUsername(), userResult -> {
                if (userResult.succeeded()) {
                    User propertyUser = userResult.result();
                    if (Objects.equals(propertyUser.password, authInfo.getPassword())) {
                        resultHandler.handle(Future.succeededFuture(io.vertx.ext.auth.User.fromName(propertyUser.name)));
                    } else {
                        resultHandler.handle(Future.failedFuture("invalid username/password"));
                    }
                } else {
                    resultHandler.handle(Future.failedFuture("invalid username/password"));
                }
            });

        } catch (RuntimeException e) {
            resultHandler.handle(Future.failedFuture(e));
        }
    }

    @Override
    public String getId() {
        // use the path as the id
        return path;
    }

    @Override
    public void getAuthorizations(io.vertx.ext.auth.User user, Handler<AsyncResult<Void>> resultHandler) {
        String username = user.principal().getString("username");
        getUser(username, userResult -> {
            if (userResult.succeeded()) {
                Set<Authorization> result = new HashSet<>();
                for (Role role : userResult.result().roles.values()) {
                    result.add(RoleBasedAuthorization.create(role.name));
                    for (String permission : role.permissions) {
                        result.add(WildcardPermissionBasedAuthorization.create(permission));
                    }
                }
                user.authorizations().add(getId(), result);
                resultHandler.handle(Future.succeededFuture());
            } else {
                resultHandler.handle(Future.failedFuture("invalid username"));
            }
        });
    }

    private static class User {
        final String name;
        String password;
        final Map<String, Role> roles;

        private User(String name) {
            this.name = Objects.requireNonNull(name);
            this.roles = new HashMap<>();
        }

        private void addRole(Role role) {
            Objects.requireNonNull(role);
            roles.put(role.name, role);
        }
    }

    private static class Role {
        final String name;
        final Set<String> permissions;

        private Role(String name) {
            this.name = Objects.requireNonNull(name);
            this.permissions = new HashSet<>();
        }

        private void addPermission(String permission) {
            Objects.requireNonNull(permission);
            permissions.add(permission);
        }
    }


}
