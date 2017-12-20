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

import com.github.zkclient.IZkChildListener;
import com.github.zkclient.IZkStateListener;
import com.github.zkclient.ZkClient;
import io.jafka.cluster.Broker;
import io.jafka.cluster.Partition;
import io.jafka.common.NoBrokersForPartitionException;
import io.jafka.utils.ZKConfig;
import io.jafka.utils.zookeeper.ZkUtils;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ZKBrokerPartitionInfo implements BrokerPartitionInfo {

    private final Logger logger = LoggerFactory.getLogger(ZKBrokerPartitionInfo.class);

    final ZKConfig zkConfig;

    final Callback callback;

    private final Object zkWatcherLock = new Object();

    private final ZkClient zkClient;

    private Map<String, SortedSet<Partition>> topicBrokerPartitions;

    private Map<Integer, Broker> allBrokers;

    private BrokerTopicsListener brokerTopicsListener;

    public ZKBrokerPartitionInfo(ZKConfig zkConfig, Callback callback) {
        this.zkConfig = zkConfig;
        this.callback = callback;
        // 创建zk的连接
        this.zkClient = new ZkClient(zkConfig.getZkConnect(), //
                zkConfig.getZkSessionTimeoutMs(), //
                zkConfig.getZkConnectionTimeoutMs());
        // 获取整个集群的broker信息
        this.allBrokers = getZKBrokerInfo();
        // 获取整个集群的topic分区信息
        // key:topic,value:Set<Partition> 按分区号从小到大排序
        this.topicBrokerPartitions = getZKTopicPartitionInfo(this.allBrokers);
        //use just the brokerTopicsListener for all watchers
        // 监听topic,broker的变化
        this.brokerTopicsListener = new BrokerTopicsListener(this.topicBrokerPartitions, this.allBrokers);

        //register listener for change of topics to keep topicsBrokerPartitions updated
        // 监听 /brokers/topics
        zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath, brokerTopicsListener);

        //register listener for change of brokers for each topic to keep topicsBrokerPartitions updated
        for (String topic : this.topicBrokerPartitions.keySet()) {
            // 监听 /brokers/topics/topic-name
            zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic, this.brokerTopicsListener);
        }
        // register listener for new broker
        // 监听 /brokers/ids
        zkClient.subscribeChildChanges(ZkUtils.BrokerIdsPath, this.brokerTopicsListener);

        // 总结：brokerTopicsListener需要监听处理 /brokers/topics  /brokers/topics/topic-name  /brokers/ids 三个目录的变化
        //
        // register listener for session expired event
        // 监听session过期
        zkClient.subscribeStateChanges(new ZKSessionExpirationListener());
    }

    public SortedSet<Partition> getBrokerPartitionInfo(final String topic) {
        synchronized (zkWatcherLock) {
            SortedSet<Partition> brokerPartitions = topicBrokerPartitions.get(topic);
            if (brokerPartitions == null || brokerPartitions.size() == 0) {
                brokerPartitions = bootstrapWithExistingBrokers(topic);
                topicBrokerPartitions.put(topic, brokerPartitions);
                return brokerPartitions;
            } else {
                return new TreeSet<Partition>(brokerPartitions);
            }
        }
    }
    
    private SortedSet<Partition> bootstrapWithExistingBrokers(String topic) {
        List<String> brokers = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath);
        if (brokers == null) {
            throw new NoBrokersForPartitionException("no brokers");
        }
        TreeSet<Partition> partitions = new TreeSet<Partition>();
        for (String brokerId : brokers) {
            partitions.add(new Partition(Integer.valueOf(brokerId), 0));
        }
        return partitions;
    }

    public Broker getBrokerInfo(int brokerId) {
        synchronized (zkWatcherLock) {
            return allBrokers.get(brokerId);
        }
    }

    public Map<Integer, Broker> getAllBrokerInfo() {
        return allBrokers;
    }

    /**
     * Generate a sequence of (brokerId, numPartitions) for all topics registered in zookeeper
     * @param allBrokers all register brokers
     * @return a mapping from topic to sequence of (brokerId, numPartitions)
     */
    private Map<String, SortedSet<Partition>> getZKTopicPartitionInfo(Map<Integer, Broker> allBrokers) {
        final Map<String, SortedSet<Partition>> brokerPartitionsPerTopic = new HashMap<String, SortedSet<Partition>>();
        ZkUtils.makeSurePersistentPathExists(zkClient, ZkUtils.BrokerTopicsPath);
        // 获取 /brokers/topics下所有的topic
        List<String> topics = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerTopicsPath);
        for (String topic : topics) {
            // find the number of broker partitions registered for this topic
            // /brokers/topics/topic-name
            String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic;
            // 获取 /brokers/topics/topic-name 下所有的brokerId
            List<String> brokerList = ZkUtils.getChildrenParentMayNotExist(zkClient, brokerTopicPath);
            // 保证有序，不重复
            final SortedSet<Partition> sortedBrokerPartitions = new TreeSet<Partition>();
            final Set<Integer> existBids = new HashSet<Integer>();
            for (String bid : brokerList) {
                // brokerId
                final int ibid = Integer.parseInt(bid);
                final String numPath = brokerTopicPath + "/" + bid;
                // 获取在broker上 该topic拥有的分区数目
                final Integer numPartition = Integer.valueOf(ZkUtils.readData(zkClient, numPath));
                for (int i = 0; i < numPartition.intValue(); i++) {
                    // i是分区id
                    sortedBrokerPartitions.add(new Partition(ibid, i));
                }
                existBids.add(ibid);
            }
            // add all brokers after topic created
            for(Integer bid:allBrokers.keySet()){
                // 说明broker是新增的，那么就需要创建在新增的broker上创建默认的分区
                if(!existBids.contains(bid)){
                    sortedBrokerPartitions.add(new Partition(bid,0));// this broker run after topic created
                }
            }
            logger.debug("Broker ids and # of partitions on each for topic: " + topic + " = " + sortedBrokerPartitions);
            brokerPartitionsPerTopic.put(topic, sortedBrokerPartitions);
        }
        return brokerPartitionsPerTopic;
    }

    /**
     * 获取整个集群的broker
     * key:brokerId,value:Broker
     * 注意：集群中brokerId是不相同的，所以不用担心brokerId重复的问题
     * @return
     */
    private Map<Integer, Broker> getZKBrokerInfo() {
        Map<Integer, Broker> brokers = new HashMap<Integer, Broker>();
        // 获取整个集群的broker
        List<String> allBrokersIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath);
        if (allBrokersIds != null) {
            logger.info("read all brokers count: " + allBrokersIds.size());
            for (String brokerId : allBrokersIds) {
                String brokerInfo = ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId);
                Broker createBroker = Broker.createBroker(Integer.valueOf(brokerId), brokerInfo);
                brokers.put(Integer.valueOf(brokerId), createBroker);
                logger.info("Loading Broker " + createBroker);
            }
        }
        return brokers;
    }

    public void updateInfo() {
        synchronized (this.zkWatcherLock) {
            this.allBrokers = getZKBrokerInfo();
            this.topicBrokerPartitions = getZKTopicPartitionInfo(this.allBrokers);
        }
    }

    public void close() {
        this.zkClient.close();
    }

    class BrokerTopicsListener implements IZkChildListener {

        private Map<String, SortedSet<Partition>> originalBrokerTopicsParitions;

        private Map<Integer, Broker> originBrokerIds;

        public BrokerTopicsListener(Map<String, SortedSet<Partition>> originalBrokerTopicsParitions,
                Map<Integer, Broker> originBrokerIds) {
            super();
            this.originalBrokerTopicsParitions = new LinkedHashMap<String, SortedSet<Partition>>(
                    originalBrokerTopicsParitions);
            this.originBrokerIds = new LinkedHashMap<Integer, Broker>(originBrokerIds);
            logger.debug("[BrokerTopicsListener] Creating broker topics listener to watch the following paths - \n" + "/broker/topics, /broker/topics/<topic>, /broker/<ids>");
            logger.debug("[BrokerTopicsListener] Initialized this broker topics listener with initial mapping of broker id to " + "partition id per topic with " + originalBrokerTopicsParitions);
        }

        /**
         * 监听topic变化
         *
         * @param parentPath
         * @param currentChilds
         * @throws Exception
         */
        public void handleChildChange(final String parentPath, List<String> currentChilds) throws Exception {
            final List<String> curChilds = currentChilds != null ? currentChilds : new ArrayList<String>();
            synchronized (zkWatcherLock) {
                // 如果变化的目录是 /brokers/topics
                if (ZkUtils.BrokerTopicsPath.equals(parentPath)) {
                    Iterator<String> updatedTopics = curChilds.iterator();
                    while (updatedTopics.hasNext()) {
                        String t = updatedTopics.next();
                        // 如果topic已经包含topic，则移除
                        if (originalBrokerTopicsParitions.containsKey(t)) {
                            updatedTopics.remove();
                        }
                    }
                    // 这里剩余的就是新创建的topic
                    for (String addedTopic : curChilds) {
                        String path = ZkUtils.BrokerTopicsPath + "/" + addedTopic;
                        // 获取新增的topic的brokerIds
                        List<String> brokerList = ZkUtils.getChildrenParentMayNotExist(zkClient, path);
                        // 更新topic的分区信息
                        processNewBrokerInExistingTopic(addedTopic, brokerList);
                        // 为新增topic的路径，绑定监听topic变化事件
                        zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + addedTopic,
                                brokerTopicsListener);
                    }
                    // 如果变化的目录是 /brokers/ids,broker发生了变化
                } else if (ZkUtils.BrokerIdsPath.equals(parentPath)) {
                    processBrokerChange(parentPath, curChilds);
                } else {
                    //check path: /brokers/topics/<topicname>
                    // 如果变化的目录是/brokers/topics/topic-name，说明某个topic-name下的broker发生了变化
                    String[] ps = parentPath.split("/");
                    if (ps.length == 4 && "topics".equals(ps[2])) {
                        logger.debug("[BrokerTopicsListener] List of brokers changed at " + parentPath + "\t Currently registered " + " list of brokers -> " + curChilds + " for topic -> " + ps[3]);
                        processNewBrokerInExistingTopic(ps[3], curChilds);
                    }
                }
                //
                //update the data structures tracking older state values
                resetState();
            }
        }


        private void processBrokerChange(String parentPath, List<String> curChilds) {
            // 复制原有的brokerIds
            final Map<Integer, Broker> oldBrokerIdMap = new HashMap<Integer, Broker>(originBrokerIds);
            for (int i = curChilds.size() - 1; i >= 0; i--) {
                Integer brokerId = Integer.valueOf(curChilds.get(i));
                // 原来的brokerIds，包含，则把当前的子节点移除
                // 这里写得有点经典！
                if (oldBrokerIdMap.remove(brokerId) != null) {//old topic
                    curChilds.remove(i);//remove old topics and left new topics
                }
            }
            //now curChilds are all new brokers
            //oldBrokerIdMap are all dead brokers
            // 这里所有的curChild都是新增的broker,所有的oldBrokerIdMap都是死掉的broker
            for (String newBroker : curChilds) {
                final String brokerInfo = ZkUtils.readData(zkClient, ZkUtils.BrokerIdsPath + "/" + newBroker);
                final Integer newBrokerId = Integer.valueOf(newBroker);
                final Broker broker = Broker.createBroker(newBrokerId.intValue(),brokerInfo);
                // 添加到所有的brokers
                allBrokers.put(newBrokerId, broker);
                // 执行创建broker的回调方法
                callback.producerCbk(broker.id, broker.host, broker.port,broker.autocreated);
            }
            //
            //remove all dead broker and remove all broker-partition from topic list
            // 移除所有死掉的broker
            for (Map.Entry<Integer, Broker> deadBroker : oldBrokerIdMap.entrySet()) {
                //remove dead broker
                allBrokers.remove(deadBroker.getKey());

                //remove dead broker-partition from topic
                // 从topic分区信息中移除broker
                for (Map.Entry<String, SortedSet<Partition>> topicParition : topicBrokerPartitions.entrySet()) {
                    Iterator<Partition> partitions = topicParition.getValue().iterator();
                    while (partitions.hasNext()) {
                        Partition p = partitions.next();
                        if (deadBroker.getKey().intValue() == p.brokerId) {
                            partitions.remove();
                        }
                    }
                }
            }
        }

        private void processNewBrokerInExistingTopic(String topic, List<String> brokerList) {
            // 获取新增topic的所有broker的每个broker的分区信息
            SortedSet<Partition> updatedBrokerParts = getBrokerPartitions(zkClient, topic, brokerList);
            // 获取老的分区信息
            SortedSet<Partition> oldBrokerParts = topicBrokerPartitions.get(topic);
            SortedSet<Partition> mergedBrokerParts = new TreeSet<Partition>();
            // 如果老分区信息存在，则需要合并分区信息
            // 合并后的分区信息并不是可靠的，可能有的broker已经不在了
            if (oldBrokerParts != null) {
                mergedBrokerParts.addAll(oldBrokerParts);
            }
            //override old parts or add new parts
            mergedBrokerParts.addAll(updatedBrokerParts);
            //
            // keep only brokers that are alive
            Iterator<Partition> iter = mergedBrokerParts.iterator();
            // 这里对broker在校验一次，如果有的broker移除了，则需要删除分区信息
            while (iter.hasNext()) {
                if (!allBrokers.containsKey(iter.next().brokerId)) {
                    iter.remove();
                }
            }
            //            mergedBrokerParts = Sets.filter(mergedBrokerParts, new Predicate<Partition>() {
            //
            //                public boolean apply(Partition input) {
            //                    return allBrokers.containsKey(input.brokerId);
            //                }
            //            });
            // 更新topic的分区信息
            topicBrokerPartitions.put(topic, mergedBrokerParts);
            logger.debug("[BrokerTopicsListener] List of broker partitions for topic: " + topic + " are " + mergedBrokerParts);
        }

        /**
         * listener本身复制topic分区信息数据到originalBrokerTopicsParitions，复制所有的broker到originBrokerIds
         */
        private void resetState() {
            logger.debug("[BrokerTopicsListener] Before reseting broker topic partitions state " + this.originalBrokerTopicsParitions);
            this.originalBrokerTopicsParitions = new HashMap<String, SortedSet<Partition>>(topicBrokerPartitions);
            logger.debug("[BrokerTopicsListener] After reseting broker topic partitions state " + originalBrokerTopicsParitions);
            //
            logger.debug("[BrokerTopicsListener] Before reseting broker id map state " + originBrokerIds);
            this.originBrokerIds = new HashMap<Integer, Broker>(allBrokers);
            logger.debug("[BrokerTopicsListener] After reseting broker id map state " + originBrokerIds);
        }
    }

    class ZKSessionExpirationListener implements IZkStateListener {

        public void handleNewSession() throws Exception {
            /**
             * When we get a SessionExpired event, we lost all ephemeral nodes and zkclient has
             * reestablished a connection for us.
             */
            logger.info("ZK expired; release old list of broker partitions for topics ");
            // session过期后，更新集群的broker信息
            allBrokers = getZKBrokerInfo();
            // session过期后，更新集群的topic分区信息
            topicBrokerPartitions = getZKTopicPartitionInfo(allBrokers);
            // 更新listener中的broker信息，topic分区信息
            brokerTopicsListener.resetState();

            // register listener for change of brokers for each topic to keep topicsBrokerPartitions updated
            // NOTE: this is probably not required here. Since when we read from getZKTopicPartitionInfo() above,
            // it automatically recreates the watchers there itself
            // 重新监听集群所有的topic /brokers/topics/topic-name
            for (String topic : topicBrokerPartitions.keySet()) {
                zkClient.subscribeChildChanges(ZkUtils.BrokerTopicsPath + "/" + topic, brokerTopicsListener);
            }
            // there is no need to re-register other listeners as they are listening on the child changes of
            // permanent nodes
        }

        public void handleStateChanged(KeeperState state) throws Exception {
        }
    }

    /**
     * Generate a mapping from broker id to (brokerId, numPartitions) for the list of brokers
     * specified
     * 
     * @param topic the topic to which the brokers have registered
     * @param brokerList the list of brokers for which the partitions info is to be generated
     * @return a sequence of (brokerId, numPartitions) for brokers in brokerList
     */
    private static SortedSet<Partition> getBrokerPartitions(ZkClient zkClient, String topic, List<?> brokerList) {
        final String brokerTopicPath = ZkUtils.BrokerTopicsPath + "/" + topic;
        final SortedSet<Partition> brokerParts = new TreeSet<Partition>();
        for (Object brokerId : brokerList) {
            final Integer bid = Integer.valueOf(brokerId.toString());
            final Integer numPartition = Integer.valueOf(ZkUtils.readData(zkClient, brokerTopicPath + "/" + bid));
            for (int i = 0; i < numPartition.intValue(); i++) {
                brokerParts.add(new Partition(bid, i));
            }
        }
        return brokerParts;
    }
}
