/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.jafka.network;

import java.io.Closeable;


import io.jafka.mx.SocketServerStats;
import io.jafka.server.Server;
import io.jafka.server.ServerConfig;
import io.jafka.utils.Closer;
import io.jafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class SocketServer implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(Server.class);

    private final RequestHandlerFactory handlerFactory;

    private final int maxRequestSize;

    //
    private final Processor[] processors;

    private final Acceptor acceptor;

    private final SocketServerStats stats;

    private final ServerConfig serverConfig;

    public SocketServer(RequestHandlerFactory handlerFactory, //
            ServerConfig serverConfig) {
        super();
        this.serverConfig = serverConfig;
        this.handlerFactory = handlerFactory;
        // max.socket.request.bytes 请求最大的字节数目，默认 10 * 1024 * 1024 字节数
        this.maxRequestSize = serverConfig.getMaxSocketRequestSize();
        // num.threads 处理客户端请求的线程数目
        this.processors = new Processor[serverConfig.getNumThreads()];
        this.stats = new SocketServerStats(1000L * 1000L * 1000L * serverConfig.getMonitoringPeriodSecs());
        // acceptor 请求接收器， processors专门用来异步处理请求
        this.acceptor = new Acceptor(serverConfig.getPort(), //
                processors, //
                // socket.send.buffer 默认 10 * 1024  10k
                serverConfig.getSocketSendBuffer(), //
                // socket.receive.buffer 默认 10 * 1024 10k
                serverConfig.getSocketReceiveBuffer());
    }

    /**
     * Shutdown the socket server
     */
    public void close() {
        Closer.closeQuietly(acceptor);
        for (Processor processor : processors) {
            Closer.closeQuietly(processor);
        }
    }

    /**
     * Start the socket server and waiting for finished
     * 
     * @throws InterruptedException thread interrupted
     */
    public void startup() throws InterruptedException {
        final int maxCacheConnectionPerThread = serverConfig.getMaxConnections() / processors.length;
        logger.debug("start {} Processor threads",processors.length);
        for (int i = 0; i < processors.length; i++) {
            processors[i] = new Processor(handlerFactory, stats, maxRequestSize, maxCacheConnectionPerThread);
            Utils.newThread("jafka-processor-" + i, processors[i], false).start();
        }
        Utils.newThread("jafka-acceptor", acceptor, false).start();
        acceptor.awaitStartup();
    }

    public SocketServerStats getStats() {
        return stats;
    }
}
