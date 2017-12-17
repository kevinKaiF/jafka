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

package io.jafka.server;

import io.jafka.consumer.Consumer;
import io.jafka.consumer.ConsumerConfig;
import io.jafka.consumer.ConsumerConnector;
import io.jafka.consumer.MessageStream;
import io.jafka.consumer.TopicEventHandler;
import io.jafka.consumer.ZookeeperTopicEventWatcher;
import io.jafka.message.Message;
import io.jafka.producer.Producer;
import io.jafka.producer.ProducerConfig;
import io.jafka.producer.serializer.MessageEncoders;
import io.jafka.utils.Closer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class EmbeddedConsumer implements TopicEventHandler<String> {

    private final ConsumerConfig consumerConfig;

    private final ServerStartable serverStartable;

    //
    private final List<String> whiteListTopics;

    private final List<String> blackListTopics;

    private final Producer<Void, Message> producer;

    //
    private ZookeeperTopicEventWatcher topicEventWatcher;

    private ConsumerConnector consumerConnector;

    private List<MirroringThread> threadList = new ArrayList<MirroringThread>();

    private static final Logger logger = LoggerFactory.getLogger(EmbeddedConsumer.class);

    private List<String> mirrorTopics = new ArrayList<String>();

    /**
     * @param consumerConfig  the inner consumer config
     * @param producerConfig  the producer config
     * @param serverStartable server instance
     */
    public EmbeddedConsumer(ConsumerConfig consumerConfig, ProducerConfig producerConfig, ServerStartable serverStartable) {
        this.consumerConfig = consumerConfig;
        this.serverStartable = serverStartable;
        //
        this.whiteListTopics = Arrays.asList(consumerConfig.getMirrorTopicsWhitelist().split(","));
        this.blackListTopics = Arrays.asList(consumerConfig.getMirrorTopicsBlackList().split(","));
        this.producer = new Producer<Void, Message>(producerConfig);
    }

    /**
     *
     */
    public void startup() {
        logger.info("staring up embedded consumer");
        topicEventWatcher = new ZookeeperTopicEventWatcher(consumerConfig, this, serverStartable);
    }

    /**
     *
     */
    public void shutdown() {
        if (topicEventWatcher != null) {
            Closer.closeQuietly(topicEventWatcher);
        }
        if (consumerConnector != null) {
            Closer.closeQuietly(consumerConnector);
        }
        for (MirroringThread thread : threadList) {
            thread.close();
        }
        producer.close();
    }

    /**
     * 处理topic变化的回调
     *
     * @param allTopics
     */
    public void handleTopicEvent(List<String> allTopics) {
        // 如果whiteListTopics包含，或者blackListTopics不包含，则添加到newMirrorTopics
        List<String> newMirrorTopics = new ArrayList<String>();
        // 如果mirrorTopics不包含topic,则添加到addedTopics
        // 新增的topic
        List<String> addedTopics = new ArrayList<String>();
        // 删除的topic
        List<String> deletedTopics = new ArrayList<String>();
        final Map<String, Integer> topicCountMap = new LinkedHashMap<String, Integer>();
        for (String topic : allTopics) {
            boolean newTopic = false;
            if (whiteListTopics.isEmpty()) {
                newTopic = whiteListTopics.contains(topic);
            } else {
                newTopic = !blackListTopics.contains(topic);
            }
            if (newTopic) {
                newMirrorTopics.add(topic);
                // mirror.consumer.numthreads 默认1
                topicCountMap.put(topic, consumerConfig.getMirrorConsumerNumThreads());
                // 如果历史topic没有包含topic，说明topic是新增的，添加到addedTopics
                if (!mirrorTopics.contains(topic)) {
                    addedTopics.add(topic);
                }
            }
        }
        //
        // 遍历历史topic mirrorTopics，如果newMirrorTopics不包含，说明需要删除该topic
        for (String topic : mirrorTopics) {
            if (!newMirrorTopics.contains(topic)) {
                deletedTopics.add(topic);
            }
        }
        // 更新
        mirrorTopics = newMirrorTopics;
        // 如果topic新增了，或者topic删除了
        if (!addedTopics.isEmpty() || !deletedTopics.isEmpty()) {
            startNewConsumerThreads(topicCountMap);
        }
    }

    private void startNewConsumerThreads(Map<String, Integer> topicCountMap) {
        if (topicCountMap.isEmpty()) return;
        if (consumerConnector != null) {
            Closer.closeQuietly(consumerConnector);
        }
        for (MirroringThread thread : threadList) {
            thread.close();
        }

        threadList.clear();

        // 创建消费端连接器，类似工厂
        consumerConnector = Consumer.create(consumerConfig);
        // key:topic,value:List<MessageStream> 每个消费线程对应一个
        // 这里的MessageStream中的queue是共享与消费线程FetcherRunnable的
        // 所以FetcherRunnable的fetchOnce的数据都会添加到queue队列中去
        Map<String, List<MessageStream<Message>>> streams = consumerConnector.createMessageStreams(topicCountMap, new MessageEncoders());
        for (Map.Entry<String, List<MessageStream<Message>>> e : streams.entrySet()) {
            int i = 0;
            // 每个FetcherRunnable都有镜像的线程
            for (MessageStream<Message> stream : e.getValue()) {
                threadList.add(new MirroringThread(stream, e.getKey(), i++, producer));
            }
        }
        for (MirroringThread t : threadList) {
            t.start();
        }
    }

}
