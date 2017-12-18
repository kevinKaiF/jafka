/*
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

package io.jafka.consumer;

import com.github.zkclient.IZkChildListener;
import com.github.zkclient.IZkStateListener;
import com.github.zkclient.ZkClient;
import com.github.zkclient.exception.ZkNoNodeException;
import com.github.zkclient.exception.ZkNodeExistsException;
import io.jafka.api.OffsetRequest;
import io.jafka.cluster.Broker;
import io.jafka.cluster.Cluster;
import io.jafka.cluster.Partition;
import io.jafka.common.ConsumerRebalanceFailedException;
import io.jafka.common.InvalidConfigException;
import io.jafka.producer.serializer.Decoder;
import io.jafka.utils.Closer;
import io.jafka.utils.KV.StringTuple;
import io.jafka.utils.Pool;
import io.jafka.utils.Scheduler;
import io.jafka.utils.zookeeper.ZkGroupDirs;
import io.jafka.utils.zookeeper.ZkGroupTopicDirs;
import io.jafka.utils.zookeeper.ZkUtils;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static java.lang.String.format;

/**
 * This class handles the consumers interaction with zookeeper
 *
 * Directories:
 * <p>
 * <b>1. Consumer id registry:</b>
 *
 * <pre>
 * /consumers/[group_id]/ids[consumer_id] -- topic1,...topicN
 * </pre>
 *
 * A consumer has a unique consumer id within a consumer group. A consumer registers its id as
 * an ephemeral znode and puts all topics that it subscribes to as the value of the znode. The
 * znode is deleted when the client is gone. A consumer subscribes to event changes of the
 * consumer id registry within its group.
 * <p>
 * The consumer id is picked up from configuration, instead of the sequential id assigned by
 * ZK. Generated sequential ids are hard to recover during temporary connection loss to ZK,
 * since it's difficult for the client to figure out whether the creation of a sequential znode
 * has succeeded or not. More details can be found at
 * (http://wiki.apache.org/hadoop/ZooKeeper/ErrorHandling)
 * <p>
 * <b>2. Broker node registry:</b>
 * <pre>
 * /brokers/[0...N] -- { "host" : "host:port",
 *                        "topics" : {"topic1": ["partition1" ... "partitionN"], ...,
 *                                    "topicN": ["partition1" ... "partitionN"] } }
 * </pre>
 * This is a list of all present broker brokers. A unique logical node id is configured on each
 * broker node. A broker node registers itself on start-up and creates a znode with the logical
 * node id under /brokers.
 *
 * The value of the znode is a JSON String that contains
 *
 * <pre>
 * (1) the host name and the port the broker is listening to,
 * (2) a list of topics that the broker serves,
 * (3) a list of logical partitions assigned to each topic on the broker.
 * </pre>
 *
 * A consumer subscribes to event changes of the broker node registry.
 *
 *
 * <p>
 * <b>3. Partition owner registry:</b>
 *
 * <pre>
 * /consumers/[group_id]/owner/[topic]/[broker_id-partition_id] -- consumer_node_id
 * </pre>
 *
 * This stores the mapping before broker partitions and consumers. Each partition is owned by a
 * unique consumer within a consumer group. The mapping is reestablished after each
 * rebalancing.
 *
 *
 * <p>
 * <b>4. Consumer offset tracking:</b>
 *
 * <pre>
 * /consumers/[group_id]/offsets/[topic]/[broker_id-partition_id] -- offset_counter_value
 * </pre>
 *
 * Each consumer tracks the offset of the latest message consumed for each partition.
 *
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ZookeeperConsumerConnector implements ConsumerConnector {

    static final FetchedDataChunk SHUTDOWN_COMMAND = new FetchedDataChunk(null, null, -1);

    private final Logger logger = LoggerFactory.getLogger(ZookeeperConsumerConnector.class);

    private final AtomicBoolean isShuttingDown = new AtomicBoolean(false);

    private final Object rebalanceLock = new Object();

    private Fetcher fetcher;

    private ZkClient zkClient;

    // key:topic,value:Pool<Partition, PartitionTopicInfo>
    // key:broker-partId,value:partitionTopicInfo
    private Pool<String, Pool<Partition, PartitionTopicInfo>> topicRegistry;

    //
    private final Pool<StringTuple, BlockingQueue<FetchedDataChunk>> queues;

    private final Scheduler scheduler = new Scheduler(1, "consumer-autocommit-", false);

    private final ConsumerConfig config;

    private final boolean enableFetcher;

    //cache for shutdown
    private List<ZKRebalancerListener<?>> rebalancerListeners = new ArrayList<ZKRebalancerListener<?>>();

    public ZookeeperConsumerConnector(ConsumerConfig config) {
        this(config, true);
    }

    public ZookeeperConsumerConnector(ConsumerConfig config, boolean enableFetcher) {
        this.config = config;
        // 是否抓取数据
        this.enableFetcher = enableFetcher;
        //
        this.topicRegistry = new Pool<String, Pool<Partition, PartitionTopicInfo>>();
        this.queues = new Pool<StringTuple, BlockingQueue<FetchedDataChunk>>();
        // 连接到zk
        connectZk();
        // 创建抓取器
        createFetcher();
        // 是否是自动commit
        if (this.config.isAutoCommit()) {
            logger.info("starting auto committer every " + config.getAutoCommitIntervalMs() + " ms");
            // autocommit.interval.ms 默认1000ms
            scheduler.scheduleWithRate(new AutoCommitTask(), config.getAutoCommitIntervalMs(),
                    config.getAutoCommitIntervalMs());
        }
    }

    /**
     *
     */
    private void createFetcher() {
        if (enableFetcher) {
            this.fetcher = new Fetcher(config, zkClient);
        }
    }

    class AutoCommitTask implements Runnable {

        public void run() {
            try {
                commitOffsets();
            } catch (Throwable e) {
                logger.error("exception during autoCommit: ", e);
            }
        }
    }

    public <T> Map<String, List<MessageStream<T>>> createMessageStreams(Map<String, Integer> topicCountMap,
                                                                        Decoder<T> decoder) {
        return consume(topicCountMap, decoder);
    }

    private <T> Map<String, List<MessageStream<T>>> consume(Map<String, Integer> topicCountMap, Decoder<T> decoder) {
        if (topicCountMap == null) {
            throw new IllegalArgumentException("topicCountMap is null");
        }
        // zk消费目录
        ZkGroupDirs dirs = new ZkGroupDirs(config.getGroupId());
        Map<String, List<MessageStream<T>>> ret = new HashMap<String, List<MessageStream<T>>>();
        String consumerUuid = config.getConsumerId();
        if (consumerUuid == null) {
            // 如果没有指定consumerId则生成随机数
            consumerUuid = generateConsumerId();
        }
        logger.info(format("create message stream by consumerid [%s] with groupid [%s]", consumerUuid,
                config.getGroupId()));
        //
        //consumerIdString => groupid_consumerid
        final String consumerIdString = config.getGroupId() + "_" + consumerUuid;
        // 消费端有一个consumerIdString,去消费多个topic
        final TopicCount topicCount = new TopicCount(consumerIdString, topicCountMap);
        // 一个topic有个多个消费线程，默认是1
        for (Map.Entry<String, Set<String>> e : topicCount.getConsumerThreadIdsPerTopic().entrySet()) {
            final String topic = e.getKey();
            final Set<String> threadIdSet = e.getValue();
            final List<MessageStream<T>> streamList = new ArrayList<MessageStream<T>>();
            for (String threadId : threadIdSet) {
                // queuedchunks.max 默认10
                // 每个topic的，每个消费线程有Queue<FetchedDataChunk>
                // 这里的stream被共享了，分别是streamList,queues
                // queuedchunks.max 最大的消费抓取块
                LinkedBlockingQueue<FetchedDataChunk> stream = new LinkedBlockingQueue<FetchedDataChunk>(
                        config.getMaxQueuedChunks());
                // key:topic + groupId_consumer_id,value:Queue<FetchedDataChunk>
                // 以topic和客户端消费线程的id为key
                queues.put(new StringTuple(topic, threadId), stream);
                // consumer.timeout.ms 客户端消费超时毫秒数
                streamList.add(new MessageStream<T>(topic, stream, config.getConsumerTimeoutMs(), decoder));
            }
            // 每个topic有个List<MessageStream<T>>
            ret.put(topic, streamList);
            logger.debug("adding topic " + topic + " and stream to map.");
        }

        //listener to consumer and partition changes
        // 重新负载均衡
        ZKRebalancerListener<T> loadBalancerListener = new ZKRebalancerListener<T>(config.getGroupId(),dirs,topicCount, consumerIdString, ret);

        this.rebalancerListeners.add(loadBalancerListener);
        // register consumer first
        // 注册消费客户端到zk
        // /consumers/group-name/ids/consumerIdString
        loadBalancerListener.registerConsumer();
        //
        //register listener for session expired event
        // 监听zk的state变化
        zkClient.subscribeStateChanges(loadBalancerListener);
        // 监听/consumers/group-name/ids 即消费端的变化
        zkClient.subscribeChildChanges(dirs.consumerRegistryDir, loadBalancerListener);
        // start the thread after watcher prepared
        // 开启线程，监听客户端消费节点的变化，准备rebalance
        loadBalancerListener.start();

        // 监听每个topic下的分区变化，准备rebalance
        for (String topic : ret.keySet()) {
            //register on broker partition path changes
            final String partitionPath = ZkUtils.BrokerTopicsPath + "/" + topic;
            zkClient.subscribeChildChanges(partitionPath, loadBalancerListener);
        }

        //explicitly grigger load balancing for this consumer
        loadBalancerListener.syncedRebalance();
        return ret;
    }

    /**
     * generate random consumerid ( hostname-currenttime-uuid.sub(8) )
     *
     * @return random consumerid
     */
    private String generateConsumerId() {
        UUID uuid = UUID.randomUUID();
        try {
            return format("%s-%d-%s", InetAddress.getLocalHost().getHostName(), //
                    System.currentTimeMillis(),//
                    Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8));
        } catch (UnknownHostException e) {
            try {
                return format("%s-%d-%s", InetAddress.getLocalHost().getHostAddress(), //
                        System.currentTimeMillis(),//
                        Long.toHexString(uuid.getMostSignificantBits()).substring(0, 8));
            } catch (UnknownHostException ex) {
                throw new IllegalArgumentException(
                        "can not generate consume id by auto, set the 'consumerid' parameter to fix this");
            }
        }
    }

    // 提交offset
    public void commitOffsets() {
        if (zkClient == null) {
            logger.error("zk client is null. Cannot commit offsets");
            return;
        }
        for (Entry<String, Pool<Partition, PartitionTopicInfo>> e : topicRegistry.entrySet()) {
            ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(config.getGroupId(), e.getKey());
            //
            for (PartitionTopicInfo info : e.getValue().values()) {
                final long lastChanged = info.getConsumedOffsetChanged().get();
                if (lastChanged == 0) {
                    logger.trace("consume offset not changed");
                    continue;
                }
                // 获取当前FetcherRunnable线程消费的offset
                final long newOffset = info.getConsumedOffset();
                //path: /consumers/<group>/offsets/<topic>/<brokerid-partition>
                // 更新到zk
                final String path = topicDirs.consumerOffsetDir + "/" + info.partition.getName();
                try {
                    ZkUtils.updatePersistentPath(zkClient, path, "" + newOffset);
                } catch (Throwable t) {
                    logger.warn("exception during commitOffsets, path=" + path + ",offset=" + newOffset, t);
                } finally {
                    // 更新完后，重置变更次数为0
                    info.resetComsumedOffsetChanged(lastChanged);
                    if (logger.isDebugEnabled()) {
                        logger.debug("Committed [" + path + "] for topic " + info);
                    }
                }
            }
            //
        }
    }

    public void close() throws IOException {
        if (isShuttingDown.compareAndSet(false, true)) {
            logger.info("ZkConsumerConnector shutting down");
            try {
                scheduler.shutdown();
                if (fetcher != null) {
                    fetcher.stopConnectionsToAllBrokers();
                }
                sendShutdownToAllQueues();
                if (config.isAutoCommit()) {
                    commitOffsets();
                }
                //waiting rebalance listener to closed and then shutdown the zkclient
                for (ZKRebalancerListener<?> listener : this.rebalancerListeners) {
                    Closer.closeQuietly(listener);
                }
                if (this.zkClient != null) {
                    this.zkClient.close();
                    zkClient = null;
                }
            } catch (Exception e) {
                logger.error("error during consumer connector shutdown", e);
            }
            logger.info("ZkConsumerConnector shut down completed");
        }
    }

    private void sendShutdownToAllQueues() {
        for (BlockingQueue<FetchedDataChunk> queue : queues.values()) {
            queue.clear();
            try {
                queue.put(SHUTDOWN_COMMAND);
            } catch (InterruptedException e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    private void connectZk() {
        logger.debug("Connecting to zookeeper instance at " + config.getZkConnect());
        this.zkClient = new ZkClient(config.getZkConnect(), config.getZkSessionTimeoutMs(),
                config.getZkConnectionTimeoutMs());
        logger.debug("Connected to zookeeper at " + config.getZkConnect());
    }

    class ZKRebalancerListener<T> implements IZkChildListener, IZkStateListener , Runnable, Closeable {

        final String group;
        final TopicCount topicCount;
        final ZkGroupDirs zkGroupDirs;

        final String consumerIdString;

        Map<String, List<MessageStream<T>>> messagesStreams;

        //
        private boolean isWatcherTriggered = false;

        private final ReentrantLock lock = new ReentrantLock();

        private final Condition cond = lock.newCondition();

        private final Thread watcherExecutorThread;

        private CountDownLatch shutDownLatch = new CountDownLatch(1);

        public ZKRebalancerListener(String group, ZkGroupDirs zkGroupDirs, TopicCount topicCount, String consumerIdString,
                                    Map<String, List<MessageStream<T>>> messagesStreams) {
            this.group = group;
            this.zkGroupDirs = zkGroupDirs;
            this.topicCount = topicCount;
            this.consumerIdString = consumerIdString;
            this.messagesStreams = messagesStreams;
            //
            this.watcherExecutorThread = new Thread(this, consumerIdString + "_watcher_executor");
        }

        public void start() {
            this.watcherExecutorThread.start();
        }

        // 监听/consumers/group-name/ids 即消费端的变化
        // 同一个group下,如果消费端增加了或者减少了，就需要进行rebalance
        public void handleChildChange(String parentPath, List<String> currentChilds) throws Exception {
            lock.lock();
            try {
                logger.info("handle consumer changed: group={} consumerId={} parentPath={} currentChilds={}",
                        group,consumerIdString,parentPath,currentChilds);
                isWatcherTriggered = true;
                cond.signalAll();
            } finally {
                lock.unlock();
            }
        }

        @Override
        public void close() {
            lock.lock();
            try {
                isWatcherTriggered = false;
                cond.signalAll();
            } finally {
                lock.unlock();
            }
            try {
                shutDownLatch.await(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                //ignore
            }
        }

        public void run() {
            logger.info("starting watcher executor thread for consumer " + consumerIdString);
            boolean doRebalance;
            while (!isShuttingDown.get()) {
                try {
                    lock.lock();
                    try {
                        if (!isWatcherTriggered) {
                            cond.await(1000, TimeUnit.MILLISECONDS);//wake up periodically so that it can check the shutdown flag
                        }
                    } finally {
                        doRebalance = isWatcherTriggered;
                        isWatcherTriggered = false;
                        lock.unlock();
                    }
                    if (doRebalance) {
                        syncedRebalance();
                    }
                } catch (Throwable t) {
                    logger.error("error during syncedRebalance", t);
                }
            }
            //
            logger.info("stopped thread " + watcherExecutorThread.getName());
            shutDownLatch.countDown();
        }

        /**
         * 重新均衡客户端的消费
         *
         * 同一个group下,如果消费端增加了或者减少了，就需要进行rebalance
         */
        private void syncedRebalance() {
            synchronized (rebalanceLock) {
                // rebalance.retries.max 如果rebalance失败，会尝试几次
                for (int i = 0; i < config.getMaxRebalanceRetries(); i++) {
                    if (isShuttingDown.get()) {//do nothing while shutting down
                        return;
                    }
                    logger.info(format("[%s] rebalancing starting. try #%d", consumerIdString, i));
                    final long start = System.currentTimeMillis();
                    boolean done = false;
                    // 获取所有的broker
                    Cluster cluster = ZkUtils.getCluster(zkClient);
                    try {
                        // 如果rebalance失败了，还需要继续rebalance几次
                        done = rebalance(cluster);
                    } catch (ZkNoNodeException znne){
                        logger.info("some consumers dispeared during rebalancing: {}",znne.getMessage());
                        registerConsumer();
                    }
                    catch (Exception e) {
                        /*
                         * occasionally, we may hit a ZK exception because the ZK state is
                         * changing while we are iterating. For example, a ZK node can
                         * disappear between the time we get all children and the time we try
                         * to get the value of a child. Just let this go since another
                         * rebalance will be triggered.
                         **/
                        logger.info("exception during rebalance ", e);
                    }
                    logger.info(format("[%s] rebalanced %s. try #%d, cost %d ms",//
                            consumerIdString, done ? "OK" : "FAILED", i, System.currentTimeMillis() - start));
                    // 如果rebalance成功则返回
                    if (done) {
                        return;
                    } else {
                        /* Here the cache is at a risk of being stale. To take future rebalancing decisions correctly, we should
                         * clear the cache */
                        logger.warn("Rebalancing attempt failed. Clearing the cache before the next rebalancing operation is triggered");
                    }
                    // 否则关闭Fetcher, 已经抓取到的数据
                    closeFetchersForQueues( messagesStreams, queues.values());
                    try {
                        // rebalance.backoff.ms rebalance回退时间 默认10000ms
                        Thread.sleep(config.getRebalanceBackoffMs());
                    } catch (InterruptedException e) {
                        logger.warn(e.getMessage());
                    }
                }
            }
            throw new ConsumerRebalanceFailedException(
                    consumerIdString + " can't rebalance after " + config.getMaxRebalanceRetries() + " retries");
        }

        private boolean rebalance(Cluster cluster) {
            // map for current consumer: topic->[ groupid-consumer-0, groupid-consumer-1 ,..., groupid-consumer-N]
            // 获取当前消费端的所有的topic的消费线程
            Map<String, Set<String>> myTopicThreadIdsMap = ZkUtils.getTopicCount(zkClient, group, consumerIdString).getConsumerThreadIdsPerTopic();
            // map for all consumers in this group: topic->[groupid-consumer1-0,...,groupid-consumerX-N]
            // 获取/consumer/group-name/ids下集群中所有的topic的消费线程
            Map<String, List<String>> consumersPerTopicMap = ZkUtils.getConsumersPerTopic(zkClient, group);
            // map for all broker-partitions for the topics in this consumerid: topic->[brokerid0-partition0,...,brokeridN-partitionN]
            // 获取/brokers/topics/topic-name集群中所有的topic的broker
            Map<String, List<String>> brokerPartitionsPerTopicMap = ZkUtils.getPartitionsForTopics(zkClient,
                    myTopicThreadIdsMap.keySet());
            /*
             * fetchers must be stopped to avoid data duplication, since if the current
             * rebalancing attempt fails, the partitions that are released could be owned by
             * another consumer. But if we don't stop the fetchers first, this consumer would
             * continue returning data for released partitions in parallel. So, not stopping
             * the fetchers leads to duplicate data.
             */
            closeFetchers(messagesStreams, myTopicThreadIdsMap);
            // 清理topic的分区信息
            // /consumer/group-name/owners/topic-name
            releasePartitionOwnership(topicRegistry);
            // 重新生成topic的分区信息
            Map<StringTuple, String> partitionOwnershipDecision = new HashMap<StringTuple, String>();
            Pool<String, Pool<Partition, PartitionTopicInfo>> currentTopicRegistry = new Pool<String, Pool<Partition, PartitionTopicInfo>>();
            for (Map.Entry<String, Set<String>> e : myTopicThreadIdsMap.entrySet()) {
                // e表示 当前消费端，每个topic有多少个消费线程
                // consumerIdString 在消费端启动后就生成了当前消费端的唯一编号
                final String topic = e.getKey();
                currentTopicRegistry.put(topic, new Pool<Partition, PartitionTopicInfo>());
                //
                ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(group, topic);
                ///////////////////////////// 对整个集群做处理 /////////////////////////
                // 获取topic在整个集群中的消费线程id
                List<String> curConsumers = consumersPerTopicMap.get(topic);
                // 获取topic整个集群中的brokers
                List<String> curBrokerPartitions = brokerPartitionsPerTopicMap.get(topic);
                // 每个消费线程应该分配的broker的数目
                final int nPartsPerConsumer = curBrokerPartitions.size() / curConsumers.size();
                // 剩余的broker的数目
                final int nConsumersWithExtraPart = curBrokerPartitions.size() % curConsumers.size();
                ///////////////////////////// 对整个集群做处理 /////////////////////////
                logger.info("Consumer {} rebalancing the following {} partitions of topic {}:\n\t{}\n\twith {} consumers:\n\t{}",//
                        consumerIdString,curBrokerPartitions.size(),topic,curBrokerPartitions,curConsumers.size(),curConsumers
                        );
                if (logger.isDebugEnabled()) {
                    StringBuilder buf = new StringBuilder(1024);
                    buf.append("[").append(topic).append("] preassigning details:");
                    // 剩余的broker数目肯定小于curConsumers.size, 因为curConsumers.size是被除数
                    for (int i = 0; i < curConsumers.size(); i++) {
                        // 这个地方非常有意思
                        // 分区分配的算法
                        /**
                         * 假如curBrokerPartition = 22; curConsumers = 4,即 4个消费线程去消费22个分区
                         * 0         4        8        12       16       20   22
                         * ----------------------------------------------------
                         * |4        |4       |4       |4       |4       |2   |
                         * ----------------------------------------------------
                         * nPartPerConsumer = 22 / 4 = 5
                         * nConsumerWithExtraPart = 22 % 4 = 2
                         * for (i : 4)
                         * i = 0, startPart = 0,  parts = 6
                         * i = 1, startPart = 6,  parts = 6
                         * i = 2, startPart = 12, parts = 5
                         * i = 3, startPart = 17, parts = 5
                         *
                         * 刚好22个分区，很好的分配到每个消费线程上，这个关键是
                         * 如果i < nConsumersWithExtraPart,
                         * 则分配到的parts = parts + 1，start = start + i
                         * 否则parts还是平均parts, start = start + nConsumersWithExtraPart
                         */
                        final int startPart = nPartsPerConsumer * i + Math.min(i, nConsumersWithExtraPart);
                        final int nParts = nPartsPerConsumer + ((i + 1 > nConsumersWithExtraPart) ? 0 : 1);
                        if (nParts > 0) {
                            for (int m = startPart; m < startPart + nParts; m++) {
                                buf.append("\n    ").append(curConsumers.get(i)).append(" ==> ")
                                        .append(curBrokerPartitions.get(m));
                            }
                        }
                    }
                    logger.debug(buf.toString());
                }
                //consumerThreadId=> groupid_consumerid-index (index from count)
                // 当前消费端，消费的topic所对应的消费线程consumerIdString
                for (String consumerThreadId : e.getValue()) {
                    // 对当前topic，消费端线程号所在index，维持与原来一致
                    /////////////////////////// 分配当前消费线程需要消费哪些分区 //////////////////////////////
                    // 这个操作很关键，需要确定当前消费端的该消费线程在整个集群中同一group下的index
                    // 因为这个index不同，分配的分区消费数量就不同
                    // 如果 index < nConsumersWithExtraPart, 那么就会在平均分配分区数目上 + 1
                    // 否则还是平均分配分区数
                    final int myConsumerPosition = curConsumers.indexOf(consumerThreadId);
                    assert (myConsumerPosition >= 0);
                    final int startPart = nPartsPerConsumer * myConsumerPosition + Math.min(myConsumerPosition,
                            nConsumersWithExtraPart);
                    final int nParts = nPartsPerConsumer + ((myConsumerPosition + 1 > nConsumersWithExtraPart) ? 0 : 1);
                    /////////////////////////// 分配当前消费线程需要消费哪些分区 //////////////////////////////
                    /*
                     * Range-partition the sorted partitions to consumers for better locality.
                     * The first few consumers pick up an extra partition, if any.
                     */
                    if (nParts <= 0) {
                        logger.warn("No broker partition of topic {} for consumer {}, {} partitions and {} consumers",topic,consumerThreadId,//
                                curBrokerPartitions.size(),curConsumers.size());
                    } else {
                        for (int i = startPart; i < startPart + nParts; i++) {
                            String brokerPartition = curBrokerPartitions.get(i);
                            logger.info("[" + consumerThreadId + "] ==> " + brokerPartition + " claimming");
                            addPartitionTopicInfo(currentTopicRegistry, topicDirs, brokerPartition, topic,
                                    consumerThreadId);
                            // record the partition ownership decision
                            // topic的partition 由哪个消费线程在消费
                            partitionOwnershipDecision.put(new StringTuple(topic, brokerPartition), consumerThreadId);
                        }
                    }
                }
            }
            //
            /*
             * move the partition ownership here, since that can be used to indicate a truly
             * successful rebalancing attempt A rebalancing attempt is completed successfully
             * only after the fetchers have been started correctly
             */
            // 重建构建 /consumer/group-name/owners/topic-name/broker-partition
            if (reflectPartitionOwnershipDecision(partitionOwnershipDecision)) {
                logger.debug("Updating the cache");
                logger.debug("Partitions per topic cache " + brokerPartitionsPerTopicMap);
                logger.debug("Consumers per topic cache " + consumersPerTopicMap);
                // 更新最新的集群分区消费信息
                topicRegistry = currentTopicRegistry;
                // 开启所有的topic的broker消费连接
                updateFetcher(cluster, messagesStreams);
                return true;
            } else {
                return false;
            }
            ////////////////////////////
        }

        private void updateFetcher(Cluster cluster, Map<String, List<MessageStream<T>>> messagesStreams2) {
            if (fetcher != null) {
                List<PartitionTopicInfo> allPartitionInfos = new ArrayList<PartitionTopicInfo>();
                for (Pool<Partition, PartitionTopicInfo> p : topicRegistry.values()) {
                    allPartitionInfos.addAll(p.values());
                }
                fetcher.startConnections(allPartitionInfos, cluster, messagesStreams2);
            }
        }

        private boolean reflectPartitionOwnershipDecision(Map<StringTuple, String> partitionOwnershipDecision) {
            // 记录已经创建成功的/consumer/group-name/owners/topic-name/broker-partition
            // 如果整个过程失败了，还可以删除
            final List<StringTuple> successfullyOwnerdPartitions = new ArrayList<StringTuple>();
            int hasPartitionOwnershipFailed = 0;
            for (Map.Entry<StringTuple, String> e : partitionOwnershipDecision.entrySet()) {
                final String topic = e.getKey().k;
                final String brokerPartition = e.getKey().v;
                final String consumerThreadId = e.getValue();
                final ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(group, topic);
                final String partitionOwnerPath = topicDirs.consumerOwnerDir + "/" + brokerPartition;
                try {
                    // 重新构建 /consumer/group-name/owners/topic-name/broker-partition
                    // data 是consumerIdString  记录由哪个线程号在消费
                    ZkUtils.createEphemeralPathExpectConflict(zkClient, partitionOwnerPath, consumerThreadId);
                    successfullyOwnerdPartitions.add(new StringTuple(topic, brokerPartition));
                } catch (ZkNodeExistsException e2) {
                    logger.warn(format("[%s] waiting [%s] to release => %s",//
                            consumerThreadId,//
                            ZkUtils.readDataMaybeNull(zkClient, partitionOwnerPath),//
                            brokerPartition));
                    hasPartitionOwnershipFailed++;
                }
            }
            //
            if (hasPartitionOwnershipFailed > 0) {
                for (StringTuple topicAndPartition : successfullyOwnerdPartitions) {
                    deletePartitionOwnershipFromZK(topicAndPartition.k, topicAndPartition.v);
                }
                return false;
            }
            return true;
        }

        private void addPartitionTopicInfo(Pool<String, Pool<Partition, PartitionTopicInfo>> currentTopicRegistry,
                                           ZkGroupTopicDirs topicDirs, String brokerPartition, String topic, String consumerThreadId) {
            // brokerId-partitionId, 对于当前topic
            Partition partition = Partition.parse(brokerPartition);
            Pool<Partition, PartitionTopicInfo> partTopicInfoMap = currentTopicRegistry.get(topic);
            // /consumer/group-name/offsets/topic-name/brokerId-partId
            // 记录当前消费线程消费topic-partition的偏移量
            final String znode = topicDirs.consumerOffsetDir + "/" + partition.getName();
            String offsetString = ZkUtils.readDataMaybeNull(zkClient, znode);
            // If first time starting a consumer, set the initial offset based on the config
            // 这个offset很有意思， 用于指定消费端消费的位置
            // 如果offset不存在
            // SMAALLES_TIME_STRING 就是从topic-partition目录第一个LogSegment开始消费
            // LARGEST_TIME_STRING 就是从topic-partition目录的最后一个LogSegment开始消费
            // 否则 接着从之前的offset继续消费
            long offset;
            if (offsetString == null) {
                // 获取topic-partition文件夹下的offset

                // SMALLES_TIME_STRING，就是获取topic-partition下第一个的LogSegment start
                if (OffsetRequest.SMALLES_TIME_STRING.equals(config.getAutoOffsetReset())) {
                    offset = earliestOrLatestOffset(topic, partition.brokerId, partition.partId,
                            OffsetRequest.EARLIES_TTIME);
                    // LARGEST_TIME_STRING，就是获取topic-partition下最后一个的LogSegment start
                } else if (OffsetRequest.LARGEST_TIME_STRING.equals(config.getAutoOffsetReset())) {
                    offset = earliestOrLatestOffset(topic, partition.brokerId, partition.partId,
                            OffsetRequest.LATES_TTIME);
                } else {
                    throw new InvalidConfigException("Wrong value in autoOffsetReset in ConsumerConfig");
                }

            } else {
                offset = Long.parseLong(offsetString);
            }
            // 对于当前topic,获取消费线程的数据块，初始化的时候设定了队列的大小
            BlockingQueue<FetchedDataChunk> queue = queues.get(new StringTuple(topic, consumerThreadId));
            // 消费的偏移量
            AtomicLong consumedOffset = new AtomicLong(offset);
            // 抓取数据的偏移量
            AtomicLong fetchedOffset = new AtomicLong(offset);
            // 构建分区信息
            PartitionTopicInfo partTopicInfo = new PartitionTopicInfo(topic,//
                    partition,//
                    queue,//
                    consumedOffset,//
                    fetchedOffset);//
            partTopicInfoMap.put(partition, partTopicInfo);
            logger.debug(partTopicInfo + " selected new offset " + offset);
        }

        private long earliestOrLatestOffset(String topic, int brokerId, int partitionId, long earliestOrLatest) {
            SimpleConsumer simpleConsumer = null;
            long producedOffset = -1;

            try {
                Cluster cluster = ZkUtils.getCluster(zkClient);
                Broker broker = cluster.getBroker(brokerId);
                if (broker == null) {
                    throw new IllegalStateException(
                            "Broker " + brokerId + " is unavailable. Cannot issue getOffsetsBefore request");
                }
                //
                //using default value???
                simpleConsumer = new SimpleConsumer(broker.host, broker.port, config.getSocketTimeoutMs(),
                        config.getSocketBufferSize());
                // 发送给broker请求，获取offset
                long[] offsets = simpleConsumer.getOffsetsBefore(topic, partitionId, earliestOrLatest, 1);
                if (offsets.length > 0) {
                    producedOffset = offsets[0];
                }
            } catch (Exception e) {
                logger.error("error in earliestOrLatestOffset() ", e);
            } finally {
                if (simpleConsumer != null) {
                    Closer.closeQuietly(simpleConsumer);
                }
            }
            return producedOffset;
        }

        private void releasePartitionOwnership(Pool<String, Pool<Partition, PartitionTopicInfo>> localTopicRegistry) {
            if (!localTopicRegistry.isEmpty()) {
                logger.info("Releasing partition ownership => " + localTopicRegistry);
                for (Map.Entry<String, Pool<Partition, PartitionTopicInfo>> e : localTopicRegistry.entrySet()) {
                    for (Partition partition : e.getValue().keySet()) {
                        deletePartitionOwnershipFromZK(e.getKey(), partition);
                    }
                }
                localTopicRegistry.clear();//clear all
            }
        }

        private void deletePartitionOwnershipFromZK(String topic, String partitionStr) {
            ZkGroupTopicDirs topicDirs = new ZkGroupTopicDirs(group, topic);
            // /consumer/group-name/owners/topic-name
            // 删除  /consumer/group-name/owners/topic-name/brokerId-partId
            final String znode = topicDirs.consumerOwnerDir + "/" + partitionStr;
            ZkUtils.deletePath(zkClient, znode);
            logger.debug("Consumer [" + consumerIdString + "] released " + znode);
        }

        private void deletePartitionOwnershipFromZK(String topic, Partition partition) {
            this.deletePartitionOwnershipFromZK(topic, partition.toString());
        }


        private void closeFetchers(Map<String, List<MessageStream<T>>> messagesStreams2,
                                   Map<String, Set<String>> myTopicThreadIdsMap) {
            // topicRegistry.values()
            List<BlockingQueue<FetchedDataChunk>> queuesToBeCleared = new ArrayList<BlockingQueue<FetchedDataChunk>>();
            for (Map.Entry<StringTuple, BlockingQueue<FetchedDataChunk>> e : queues.entrySet()) {
                // 如果当前消费线程中包含queue中topic,则需要清理
                if (myTopicThreadIdsMap.containsKey(e.getKey().k)) {
                    queuesToBeCleared.add(e.getValue());
                }
            }
            closeFetchersForQueues( messagesStreams2, queuesToBeCleared);
        }

        private void closeFetchersForQueues(Map<String, List<MessageStream<T>>> messageStreams,
                                            Collection<BlockingQueue<FetchedDataChunk>> queuesToBeCleared) {
            if (fetcher == null) {
                return;
            }
            // 关闭与所有broker的connection
            fetcher.stopConnectionsToAllBrokers();
            // 关闭Fetcher,清理内存数据
            fetcher.clearFetcherQueues(queuesToBeCleared, messageStreams.values());
            if (config.isAutoCommit()) {
                logger.info("Committing all offsets after clearing the fetcher queues");
                commitOffsets();
            }
        }

        // 清理topic的broker分区消费信息
        private void resetState() {
            topicRegistry.clear();
        }
        //

        @Override
        public void handleNewSession() throws Exception {
            //Called after the zookeeper session has expired and a new session has been created. You would have to re-create
            // any ephemeral nodes here.
            //
            /*
             * When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has
             * reestablished a connection for us. We need to release the ownership of the
             * current consumer and re-register this consumer in the consumer registry and
             * trigger a rebalance.
             */
            logger.info("Zk expired; release old broker partition ownership; re-register consumer " + consumerIdString);
            this.resetState();
            this.registerConsumer();
            //explicitly trigger load balancing for this consumer
            this.syncedRebalance();
            //
            // There is no need to resubscribe to child and state changes.
            // The child change watchers will be set inside rebalance when we read the children list.
        }

        @Override
        public void handleStateChanged(KeeperState state) throws Exception {
            // nothing to do
        }
        ///////////////////////////////////////////////////////////
        /**
         * register the consumer in the zookeeper
         * <p>
         *     path: /consumers/groupid/ids/groupid-consumerid
         *     data: {topic:count,topic:count}
         * </p>
         */
        void registerConsumer(){
            final String path = zkGroupDirs.consumerRegistryDir + "/" + consumerIdString;
            // topicCountMap转为json数据
            final String data = topicCount.toJsonString();
            boolean ok = true;
            try {
                // 创建/consumers/group-name/ids/consumerIdString
                // data为topicCountMap的json
                ZkUtils.createEphemeralPathExpectConflict(zkClient, path, data);
            }catch (Exception ex){
                ok = false;
            }
            logger.info(format("register consumer in zookeeper [%s] => [%s] success?=%s", path, data, ok));
        }
    }

}
