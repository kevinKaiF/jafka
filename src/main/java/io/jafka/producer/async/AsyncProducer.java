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

package io.jafka.producer.async;


import io.jafka.api.ProducerRequest;
import io.jafka.common.AsyncProducerInterruptedException;
import io.jafka.common.QueueClosedException;
import io.jafka.common.QueueFullException;
import io.jafka.mx.AsyncProducerQueueSizeStats;
import io.jafka.mx.AsyncProducerStats;
import io.jafka.producer.ProducerConfig;
import io.jafka.producer.SyncProducer;
import io.jafka.producer.serializer.Encoder;
import io.jafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class AsyncProducer<T> implements Closeable {

    private final Logger logger = LoggerFactory.getLogger(AsyncProducer.class);
    private static final Random random = new Random();
    private static final String ProducerQueueSizeMBeanName = "jafka.producer.Producer:type=AsyncProducerQueueSizeStats";
    /////////////////////////////////////////////////////////////////////
    private final SyncProducer producer;


    private final CallbackHandler<T> callbackHandler;
    /////////////////////////////////////////////////////////////////////
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final LinkedBlockingQueue<QueueItem<T>> queue;
    private final int asyncProducerID = AsyncProducer.random.nextInt();
    /////////////////////////////////////////////////////////////////////
    private final ProducerSendThread<T> sendThread;
    private final int enqueueTimeoutMs;

    public AsyncProducer(AsyncProducerConfig config, //
                         SyncProducer producer, //
                         Encoder<T> serializer, //
                         EventHandler<T> eventHandler,//
                         Properties eventHandlerProperties, //
                         CallbackHandler<T> callbackHandler, //
                         Properties callbackHandlerProperties) {
        super();
        this.producer = producer;
        this.callbackHandler = callbackHandler;
        // queue.enqueueTimeout.ms 入队等待时间，默认是0ms
        this.enqueueTimeoutMs = config.getEnqueueTimeoutMs();
        //
        this.queue = new LinkedBlockingQueue<QueueItem<T>>(config.getQueueSize());
        //
        if (eventHandler != null) {
            eventHandler.init(eventHandlerProperties);
        }
        if (callbackHandler != null) {
            callbackHandler.init(callbackHandlerProperties);
        }

        // 开启异步发送线程，消费queue中的数据
        this.sendThread = new ProducerSendThread<T>("ProducerSendThread-" + asyncProducerID,
                queue, //
                serializer,//
                producer, //
                eventHandler != null ? eventHandler//
                        : new DefaultEventHandler<T>(new ProducerConfig(config.getProperties()), callbackHandler), //
                callbackHandler, //
                config.getQueueTime(), //
                config.getBatchSize());     // batch.size 批量大小，默认200
        this.sendThread.setDaemon(false);
        AsyncProducerQueueSizeStats<T> stats = new AsyncProducerQueueSizeStats<T>(queue);
        stats.setMbeanName(ProducerQueueSizeMBeanName + "-" + asyncProducerID);
        Utils.registerMBean(stats);
    }

    @SuppressWarnings("unchecked")
    public AsyncProducer(AsyncProducerConfig config) {
        this(config//
                , new SyncProducer(config)//
                , (Encoder<T>) Utils.getObject(config.getSerializerClass())//
                , (EventHandler<T>) Utils.getObject(config.getEventHandler())//
                , config.getEventHandlerProperties()//
                , (CallbackHandler<T>) Utils.getObject(config.getCbkHandler())//
                , config.getCbkHandlerProperties());
    }

    public void start() {
        sendThread.start();
    }

    public void send(String topic, T event) {
        send(topic, event, ProducerRequest.RandomPartition);
    }

    /**
     * event实际上是数据，需要发送的数据
     *
     * @param topic
     * @param event
     * @param partition
     */
    public void send(String topic, T event, int partition) {
        AsyncProducerStats.recordEvent();
        if (closed.get()) {
            throw new QueueClosedException("Attempt to add event to a closed queue.");
        }
        QueueItem<T> data = new QueueItem<T>(event, partition, topic);
        // callbackHandler是异步调用的回调方法，这样可以实现一些个性化的需求，比如监控，记录，过滤等等
        if (this.callbackHandler != null) {
            data = this.callbackHandler.beforeEnqueue(data);
        }

        boolean added = false;
        if (data != null) {
            try {
                // 如果入队不需要等待，则直接添加到队列，但是可能添加会失败
                if (enqueueTimeoutMs == 0) {
                    // offer 不会阻塞
                    added = queue.offer(data);
                    // 阻塞等待
                } else if (enqueueTimeoutMs < 0) {
                    queue.put(data);
                    added = true;
                } else {
                    // 阻塞固定时间
                    added = queue.offer(data, enqueueTimeoutMs, TimeUnit.MILLISECONDS);
                }
            } catch (InterruptedException e) {
                throw new AsyncProducerInterruptedException(e);
            }
        }
        if (this.callbackHandler != null) {
            this.callbackHandler.afterEnqueue(data, added);
        }

        if (!added) {
            AsyncProducerStats.recordDroppedEvents();
            throw new QueueFullException("Event queue is full of unsent messages, could not send event: " + event);
        }

    }

    public void close() {
        if (this.callbackHandler != null) {
            callbackHandler.close();
        }
        closed.set(true);
        sendThread.shutdown();
        sendThread.awaitShutdown();
        producer.close();
        logger.info("Closed AsyncProducer");
    }
}
