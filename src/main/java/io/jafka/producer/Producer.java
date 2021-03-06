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

package io.jafka.producer;


import io.jafka.api.ProducerRequest;
import io.jafka.cluster.Broker;
import io.jafka.cluster.Partition;
import io.jafka.common.InvalidPartitionException;
import io.jafka.common.NoBrokersForPartitionException;
import io.jafka.common.annotations.ClientSide;
import io.jafka.producer.async.CallbackHandler;
import io.jafka.producer.async.EventHandler;
import io.jafka.producer.serializer.Encoder;
import io.jafka.utils.Closer;
import io.jafka.utils.Utils;
import io.jafka.utils.ZKConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Message producer
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
@ClientSide
public class Producer<K, V> implements BrokerPartitionInfo.Callback, IProducer<K, V> {

    ProducerConfig config;

    private Partitioner<K> partitioner;

    ProducerPool<V> producerPool;

    boolean populateProducerPool;

    BrokerPartitionInfo brokerPartitionInfo;

    private final Logger logger = LoggerFactory.getLogger(Producer.class);

    /////////////////////////////////////////////////////////////////////////
    private final AtomicBoolean hasShutdown = new AtomicBoolean(false);

    private final Random random = new Random();

    private final boolean zkEnabled;
    private Encoder<V> encoder;

    public Producer(ProducerConfig config, Partitioner<K> partitioner, ProducerPool<V> producerPool, boolean populateProducerPool,
                    BrokerPartitionInfo brokerPartitionInfo) {
        super();
        this.config = config;
        this.partitioner = partitioner;
        if (producerPool == null) {
            producerPool = new ProducerPool<V>(config, getEncoder());
        }
        this.producerPool = producerPool;
        this.populateProducerPool = populateProducerPool;
        this.brokerPartitionInfo = brokerPartitionInfo;
        //
        this.zkEnabled = config.getZkConnect() != null;
        if (this.brokerPartitionInfo == null) {
            // 如果启用zk了，则监听整个集群brokers的变化，
            // /brokers/topics  /brokers/topics/topic-name  /brokers/ids
            if (this.zkEnabled) {
                Properties zkProps = new Properties();
                zkProps.put("zk.connect", config.getZkConnect());
                zkProps.put("zk.sessiontimeout.ms", "" + config.getZkSessionTimeoutMs());
                zkProps.put("zk.connectiontimeout.ms", "" + config.getZkConnectionTimeoutMs());
                zkProps.put("zk.synctime.ms", "" + config.getZkSyncTimeMs());
                this.brokerPartitionInfo = new ZKBrokerPartitionInfo(new ZKConfig(zkProps), this);
            } else {
                // 配置监听某些broker
                this.brokerPartitionInfo = new ConfigBrokerPartitionInfo(config);
            }
        }
        //
        // pool of producers, one per broker
        // 是否需要填充到producer对象池
        if (this.populateProducerPool) {
            for (Map.Entry<Integer, Broker> e : this.brokerPartitionInfo.getAllBrokerInfo().entrySet()) {
                Broker b = e.getValue();
                // 为每个broker创建一个producer
                producerPool.addProducer(new Broker(e.getKey(), b.host, b.host, b.port,b.autocreated));
            }
        }
    }

    /**
     * This constructor can be used when all config parameters will be
     * specified through the ProducerConfig object
     *
     * @param config Producer Configuration object
     */
    // 默认创建的producer, 需要填充producer对象池
    public Producer(ProducerConfig config) {
        this(config, //
                null,//
                null, //
                true, //
                null);
    }

    /**
     * This constructor can be used to provide pre-instantiated objects for
     * all config parameters that would otherwise be instantiated via
     * reflection. i.e. encoder, partitioner, event handler and callback
     * handler. If you use this constructor, encoder, eventHandler,
     * callback handler and partitioner will not be picked up from the
     * config.
     *
     * @param config       Producer Configuration object
     * @param encoder      Encoder used to convert an object of type V to a
     *                     jafka.message.Message. If this is null it throws an
     *                     InvalidConfigException
     * @param eventHandler the class that implements
     *                     jafka.producer.async.IEventHandler[T] used to dispatch a
     *                     batch of produce requests, using an instance of
     *                     jafka.producer.SyncProducer. If this is null, it uses the
     *                     DefaultEventHandler
     * @param cbkHandler   the class that implements
     *                     jafka.producer.async.CallbackHandler[T] used to inject
     *                     callbacks at various stages of the
     *                     jafka.producer.AsyncProducer pipeline. If this is null, the
     *                     producer does not use the callback handler and hence does not
     *                     invoke any callbacks
     * @param partitioner  class that implements the
     *                     jafka.producer.Partitioner[K], used to supply a custom
     *                     partitioning strategy on the message key (of type K) that is
     *                     specified through the ProducerData[K, T] object in the send
     *                     API. If this is null, producer uses DefaultPartitioner
     */
    public Producer(ProducerConfig config, Encoder<V> encoder, EventHandler<V> eventHandler, CallbackHandler<V> cbkHandler, Partitioner<K> partitioner) {
        this(config, //
                partitioner,//
                new ProducerPool<V>(config, encoder, eventHandler, cbkHandler), //
                true, //
                null);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Encoder<V> getEncoder() {
        return encoder == null ? (Encoder<V>) Utils.getObject(config.getSerializerClass()) : encoder;
    }

    public void send(ProducerData<K, V> data) throws NoBrokersForPartitionException, InvalidPartitionException {
        if (data == null) return;
        if (zkEnabled) {
            zkSend(data);
        } else {
            // 如果producer是特定配置的
            configSend(data);
        }
    }


    private void configSend(ProducerData<K, V> data) {
        producerPool.send(create(data));
    }


    private void zkSend(ProducerData<K, V> data) {
        int numRetries = 0;
        Broker brokerInfoOpt = null;
        Partition brokerIdPartition = null;
        // zk.read.num.retries 尝试次数，默认是3
        while (numRetries <= config.getZkReadRetries() && brokerInfoOpt == null) {
            if (numRetries > 0) {
                logger.info("Try #" + numRetries + " ZK producer cache is stale. Refreshing it by reading from ZK again");
                // 更新broker的分区信息
                brokerPartitionInfo.updateInfo();
            }
            // 获取该topic对应的所有分区信息
            List<Partition> partitions = new ArrayList<Partition>(getPartitionListForTopic(data));
            // 分配broker
            // 这一步非常关键，需要哪个broker的哪个partition上去
            brokerIdPartition = partitions.get(getPartition(data.getKey(), partitions.size()));
            if (brokerIdPartition != null) {
                // 获取对应broker的信息
                brokerInfoOpt = brokerPartitionInfo.getBrokerInfo(brokerIdPartition.brokerId);
            }
            numRetries++;
        }
        if (brokerInfoOpt == null) {
            throw new NoBrokersForPartitionException("Invalid Zookeeper state. Failed to get partition for topic: " + data.getTopic() + " and key: "
                    + data.getKey());
        }
        // 组装数据发送
        ProducerPoolData<V> ppd = producerPool.getProducerPoolData(data.getTopic(),//
                new Partition(brokerIdPartition.brokerId, brokerIdPartition.partId),//
                data.getData());
        producerPool.send(ppd);
    }

    /**
     * 分配需要发送到broker的分区
     *
     * @param key
     * @param numPartitions  topic所有分区的数目
     * @return
     */
    private int getPartition(K key, int numPartitions) {
        if (numPartitions <= 0) {
            throw new InvalidPartitionException("Invalid number of partitions: " + numPartitions + "\n Valid values are > 0");
        }
        // 如果key是null,随机分配分区，那么整个kafka的数据就是无序的
        // 否则按分区器分配分区
        int partition = key == null ? random.nextInt(numPartitions) : getPartitioner().partition(key, numPartitions);
        if (partition < 0 || partition >= numPartitions) {
            throw new InvalidPartitionException("Invalid partition id : " + partition + "\n Valid values are in the range inclusive [0, " + (numPartitions - 1)
                    + "]");
        }
        return partition;
    }

    public void producerCbk(int bid, String host, int port,boolean autocreated) {
        if (populateProducerPool) {
            producerPool.addProducer(new Broker(bid, host, host, port,autocreated));
        } else {
            logger.debug("Skipping the callback since populateProducerPool = false");
        }
    }

    private ProducerPoolData<V> create(ProducerData<K, V> pd) {
        // 先找ProducerData数据中的topic的分区信息
        Collection<Partition> topicPartitionsList = getPartitionListForTopic(pd);
        //FIXME: random Broker???
        // 随机找到一个broker
        int randomBrokerId = random.nextInt(topicPartitionsList.size());
        final Partition brokerIdPartition = new ArrayList<Partition>(topicPartitionsList).get(randomBrokerId);
        // 组装成新的数据，topic,partition,data
        return this.producerPool.getProducerPoolData(pd.getTopic(),//
                new Partition(brokerIdPartition.brokerId, ProducerRequest.RandomPartition), pd.getData());
    }

    private Collection<Partition> getPartitionListForTopic(ProducerData<K, V> pd) {
        SortedSet<Partition> topicPartitionsList = brokerPartitionInfo.getBrokerPartitionInfo(pd.getTopic());
        if (topicPartitionsList.size() == 0) {
            throw new NoBrokersForPartitionException("Partition= " + pd.getTopic());
        }
        return topicPartitionsList;
    }

    public void close() {
        if (hasShutdown.compareAndSet(false, true)) {
            Closer.closeQuietly(producerPool);
            Closer.closeQuietly(brokerPartitionInfo);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public Partitioner<K> getPartitioner() {
        if (partitioner == null) {
            // partitioner.class 默认DefaultPartitioner key为null，则随机，否则hash
            // 获取分区规则Partitioner
            partitioner = (Partitioner<K>) Utils.getObject(config.getPartitionerClass());
        }
        return partitioner;
    }

    public void setPartitioner(Partitioner<K> partitioner) {
        this.partitioner = partitioner;
    }
}
