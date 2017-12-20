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

package io.jafka.utils.zookeeper;


import com.github.zkclient.ZkClient;
import com.github.zkclient.exception.ZkNoNodeException;
import com.github.zkclient.exception.ZkNodeExistsException;
import io.jafka.cluster.Broker;
import io.jafka.cluster.Cluster;
import io.jafka.consumer.TopicCount;
import io.jafka.utils.Utils;

import java.util.*;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class ZkUtils {

    public static final String ConsumersPath = "/consumers";

    public static final String BrokerIdsPath = "/brokers/ids";

    public static final String BrokerTopicsPath = "/brokers/topics";


    public static void makeSurePersistentPathExists(ZkClient zkClient, String path) {
        if (!zkClient.exists(path)) {
            zkClient.createPersistent(path, true);
        }
    }

    /**
     * get children nodes name
     *
     * @param zkClient zkClient
     * @param path     full path
     * @return children nodes name or null while path not exist
     */
    public static List<String> getChildrenParentMayNotExist(ZkClient zkClient, String path) {
        try {
            return zkClient.getChildren(path);
        } catch (ZkNoNodeException e) {
            return null;
        }
    }

    public static String readData(ZkClient zkClient, String path) {
        return Utils.fromBytes(zkClient.readData(path));
    }

    public static String readDataMaybeNull(ZkClient zkClient, String path) {
        return Utils.fromBytes(zkClient.readData(path, true));
    }

    public static void updatePersistentPath(ZkClient zkClient, String path, String data) {
        try {
            zkClient.writeData(path, Utils.getBytes(data));
        } catch (ZkNoNodeException e) {
            createParentPath(zkClient, path);
            try {
                zkClient.createPersistent(path, Utils.getBytes(data));
            } catch (ZkNodeExistsException e2) {
                zkClient.writeData(path, Utils.getBytes(data));
            }
        }
    }

    private static void createParentPath(ZkClient zkClient, String path) {
        String parentDir = path.substring(0, path.lastIndexOf('/'));
        if (parentDir.length() != 0) {
            zkClient.createPersistent(parentDir, true);
        }
    }

    /**
     * read all brokers in the zookeeper
     *
     * @param zkClient zookeeper client
     * @return all brokers
     */
    public static Cluster getCluster(ZkClient zkClient) {
        Cluster cluster = new Cluster();
        List<String> nodes = getChildrenParentMayNotExist(zkClient, BrokerIdsPath);
        for (String node : nodes) {
            final String brokerInfoString = readData(zkClient, BrokerIdsPath + "/" + node);
            cluster.add(Broker.createBroker(Integer.valueOf(node), brokerInfoString));
        }
        return cluster;
    }

    public static TopicCount getTopicCount(ZkClient zkClient, String group, String consumerId) {
        ZkGroupDirs dirs = new ZkGroupDirs(group);
        String topicCountJson = ZkUtils.readData(zkClient, dirs.consumerRegistryDir + "/" + consumerId);
        return TopicCount.parse(consumerId, topicCountJson);
    }

    /**
     * read broker info for watching topics
     *
     * @param zkClient the zookeeper client
     * @param topics   topic names
     * @return topic-&gt;(brokerid-0,brokerid-1...brokerid2-0,brokerid2-1...)
     *
     * key:topic,value:brokerid-0,brokerid-1...brokerid2-0,brokerid2-1...
     */
    public static Map<String, List<String>> getPartitionsForTopics(ZkClient zkClient, Collection<String> topics) {
        Map<String, List<String>> ret = new HashMap<String, List<String>>();
        for (String topic : topics) {
            List<String> partList = new ArrayList<String>();
            // 获取/brokers/topics/topic-name下所有的broker
            List<String> brokers = getChildrenParentMayNotExist(zkClient, BrokerTopicsPath + "/" + topic);
            if (brokers != null) {
                for (String broker : brokers) {
                    // 获取brokers/topics/topic-name/broker-name的data
                    // parts broker的个数
                    final String parts = readData(zkClient, BrokerTopicsPath + "/" + topic + "/" + broker);
                    // 获取到分区数目后，就可以知道每个topic，对应的topic分区的文件夹名称了
                    int nParts = Integer.parseInt(parts);
                    for (int i = 0; i < nParts; i++) {
                        partList.add(broker + "-" + i);
                    }
                }
            }
            Collections.sort(partList);
            ret.put(topic, partList);
        }
        return ret;
    }

    /**
     * get all consumers for the group
     *
     * @param zkClient the zookeeper client
     * @param group    the group name
     * @return topic-&gt;(consumerIdStringA-0,consumerIdStringA-1...consumerIdStringB-0,consumerIdStringB-1)
     *
     * key:topic,value:consumerIdStringA-0,consumerIdStringA-1...consumerIdStringB-0,consumerIdStringB-1
     */
    public static Map<String, List<String>> getConsumersPerTopic(ZkClient zkClient, String group) {
        ZkGroupDirs dirs = new ZkGroupDirs(group);
        // 获取当前group下所有的consumerId
        List<String> consumers = getChildrenParentMayNotExist(zkClient, dirs.consumerRegistryDir);
        //
        Map<String, List<String>> consumersPerTopicMap = new HashMap<String, List<String>>();
        // 遍历consumerId
        for (String consumer : consumers) {
            // 获取每个consumerId, /consumer/group-name/ids/consumerIdString的data
            // data为topicCountMap, topicCountMap定义了每个consumer消费的线程个数
            TopicCount topicCount = getTopicCount(zkClient, group, consumer);
            // key:topic,value:Set<string> 消费端线程数目 consumerIdString-0,consumerIdString-1..
            for (Map.Entry<String, Set<String>> e : topicCount.getConsumerThreadIdsPerTopic().entrySet()) {
                // 对每一个topic做处理，将Set<String>转为list
                final String topic = e.getKey();
                for (String consumerThreadId : e.getValue()) {
                    List<String> list = consumersPerTopicMap.get(topic);
                    if (list == null) {
                        list = new ArrayList<String>();
                        consumersPerTopicMap.put(topic, list);
                    }
                    //
                    list.add(consumerThreadId);
                }
            }
        }
        // 对每个topic的消费线程名排序,很重要
        for (Map.Entry<String, List<String>> e : consumersPerTopicMap.entrySet()) {
            Collections.sort(e.getValue());
        }
        return consumersPerTopicMap;
    }
    //

    public static void deletePath(ZkClient zkClient, String path) {
        try {
            zkClient.delete(path);
        } catch (ZkNoNodeException e) {
        }
    }

    public static String readDataMaybyNull(ZkClient zkClient, String path) {
        return Utils.fromBytes(zkClient.readData(path, true));
    }

    /**
     * Create an ephemeral node with the given path and data. Create parents if necessary.
     * @param zkClient client of zookeeper
     * @param path node path of zookeeper
     * @param data node data
     */
    public static void createEphemeralPath(ZkClient zkClient, String path, String data) {
        try {
            zkClient.createEphemeral(path, Utils.getBytes(data));
        } catch (ZkNoNodeException e) {
            createParentPath(zkClient, path);
            zkClient.createEphemeral(path, Utils.getBytes(data));
        }
    }

    public static void createEphemeralPathExpectConflict(ZkClient zkClient, String path, String data) {
        try {
            createEphemeralPath(zkClient, path, data);
        } catch (ZkNodeExistsException e) {
            //this can happend when there is connection loss;
            //make sure the data is what we intend to write
            String storedData = null;
            try {
                storedData = readData(zkClient, path);
            } catch (ZkNoNodeException e2) {
                //ignore
            }
            if (storedData == null || !storedData.equals(data)) {
                throw new ZkNodeExistsException("conflict in " + path + " data: " + data + " stored data: " + storedData);
            }
            //
            //otherwise, the creation succeeded, return normally
        }
    }
}
