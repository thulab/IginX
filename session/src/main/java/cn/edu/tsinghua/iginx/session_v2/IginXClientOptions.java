/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package cn.edu.tsinghua.iginx.session_v2;


public final class IginXClientOptions {

    private static final String DEFAULT_USERNAME = "root";

    private static final String DEFAULT_PASSWORD = "root";

    private final String host;

    private final int port;

    private final String username;

    private final String password;

    private IginXClientOptions(IginXClientOptions.Builder builder) {
        this.host = builder.host;
        this.port = builder.port;
        this.username = builder.username;
        this.password = builder.password;
    }

    public static IginXClientOptions.Builder builder() {
        return new IginXClientOptions.Builder();
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getUsername() {
        return username;
    }

    public String getPassword() {
        return password;
    }

    public static class Builder {

        private String host;

        private int port;

        private String username;

        private String password;

        private Builder() {
        }

        public IginXClientOptions.Builder url(String url) {
            Arguments.checkUrl(url, "url");
            // TODO: 将 url 拆分成 host + port
            String[] dividedStr = url.split(":");
            this.host = dividedStr[0];
            this.port = Integer.parseInt(dividedStr[1]);
            return this;
        }

        public IginXClientOptions.Builder host(String host) {
            Arguments.checkNonEmpty(host, "host");
            this.host = host;
            return this;
        }

        public IginXClientOptions.Builder port(int port) {
            this.port = port;
            return this;
        }

        public IginXClientOptions.Builder authenticate(String username, String password) {
            Arguments.checkNonEmpty(username, "username");
            Arguments.checkNonEmpty(password, "password");
            this.username = username;
            this.password = password;
            return this;
        }

        public IginXClientOptions.Builder username(String username) {
            Arguments.checkNonEmpty(username, "username");
            this.username = username;
            return this;
        }

        public IginXClientOptions.Builder password(String password) {
            Arguments.checkNonEmpty(password, "password");
            this.password = password;
            return this;
        }

        public IginXClientOptions build() {
            if (this.host == null || this.port == 0) {
                throw new IllegalStateException("the host and port to connect to Iginx has to be defined.");
            }
            if (this.username == null) {
                this.username = DEFAULT_USERNAME;
            }
            if (this.password == null) {
                this.password = DEFAULT_PASSWORD;
            }
            return new IginXClientOptions(this);
        }

    }

}
