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

package io.jafka.log;

import io.jafka.api.OffsetRequest;
import io.jafka.api.PartitionChooser;
import io.jafka.common.InvalidPartitionException;
import io.jafka.server.ServerConfig;
import io.jafka.server.ServerRegister;
import io.jafka.server.TopicTask;
import io.jafka.utils.Closer;
import io.jafka.utils.IteratorTemplate;
import io.jafka.utils.KV;
import io.jafka.utils.Pool;
import io.jafka.utils.Scheduler;
import io.jafka.utils.TopicNameValidator;
import io.jafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;

import static java.lang.String.format;

/**
 * The log management system is responsible for log creation, retrieval, and cleaning.
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class LogManager implements PartitionChooser, Closeable {

    final ServerConfig config;

    private final Scheduler scheduler;

    final long logCleanupIntervalMs;

    final long logCleanupDefaultAgeMs;

    final boolean needRecovery;

    private final Logger logger = LoggerFactory.getLogger(LogManager.class);

    ///////////////////////////////////////////////////////////////////////
    final int numPartitions;

    final File logDir;

    final int flushInterval;

    private final Object logCreationLock = new Object();

    final Random random = new Random();

    final CountDownLatch startupLatch;

    // key:topic,Value:Pool<Integer, Log>
    // key:partition,Value:Log
    private final Pool<String, Pool<Integer, Log>> logs = new Pool<String, Pool<Integer, Log>>();

    // 日志刷出调度调度器，只使用单线程
    private final Scheduler logFlusherScheduler = new Scheduler(1, "jafka-logflusher-", false);

    //
    private final LinkedBlockingQueue<TopicTask> topicRegisterTasks = new LinkedBlockingQueue<TopicTask>();

    private volatile boolean stopTopicRegisterTasks = false;

    final Map<String, Integer> logFlushIntervalMap;

    final Map<String, Long> logRetentionMSMap;

    final int logRetentionSize;

    /////////////////////////////////////////////////////////////////////////
    private ServerRegister serverRegister;

    // key:topic,Value:partitionNum(分区数目)
    private final Map<String, Integer> topicPartitionsMap;

    private RollingStrategy rollingStategy;

    private final int maxMessageSize;

    public LogManager(ServerConfig config, //
                      Scheduler scheduler, //
                      long logCleanupIntervalMs, //
                      long logCleanupDefaultAgeMs, //
                      boolean needRecovery) {
        super();
        this.config = config;
        this.maxMessageSize = config.getMaxMessageSize();
        this.scheduler = scheduler;
        //        this.time = time;
        // log.cleanup.interval.mins
        this.logCleanupIntervalMs = logCleanupIntervalMs;
        // log.retention.hours 默认是24 * 7小时，一周
        this.logCleanupDefaultAgeMs = logCleanupDefaultAgeMs;
        this.needRecovery = needRecovery;
        // log.dir 日志文件所在的位置
        this.logDir = Utils.getCanonicalFile(new File(config.getLogDir()));
        // num.partitions 分区数目
        this.numPartitions = config.getNumPartitions();
        // log.flush.interval 默认10000个 内存中MessageAndOffset的个数
        this.flushInterval = config.getFlushInterval();
        this.topicPartitionsMap = config.getTopicPartitionsMap();
        this.startupLatch = config.getEnableZookeeper() ? new CountDownLatch(1) : null;
        this.logFlushIntervalMap = config.getFlushIntervalMap();
        // log.retention.size 保留文件的最大值
        this.logRetentionSize = config.getLogRetentionSize();
        this.logRetentionMSMap = getLogRetentionMSMap(config.getLogRetentionHoursMap());
        //
    }

    public void setRollingStategy(RollingStrategy rollingStategy) {
        this.rollingStategy = rollingStategy;
    }

    /**
     * 加载日志
     *
     * @throws IOException
     */
    public void load() throws IOException {
        // 默认的rolling策略是按文件固定大小rolling
        if (this.rollingStategy == null) {
            this.rollingStategy = new FixedSizeRollingStrategy(config.getLogFileSize());
        }
        if (!logDir.exists()) {
            logger.info("No log directory found, creating '" + logDir.getAbsolutePath() + "'");
            logDir.mkdirs();
        }
        if (!logDir.isDirectory() || !logDir.canRead()) {
            throw new IllegalArgumentException(logDir.getAbsolutePath() + " is not a readable log directory.");
        }

        // 获取指定日志目录下的所有子文件夹
        // 加载所有的日志文件信息到内存
        File[] subDirs = logDir.listFiles();
        if (subDirs != null) {
            for (File dir : subDirs) {
                // 子文件的文件肯定是文件，而不是文件夹
                if (!dir.isDirectory()) {
                    logger.warn("Skipping unexplainable file '" + dir.getAbsolutePath() + "'--should it be there?");
                } else {
                    logger.info("Loading log from " + dir.getAbsolutePath());
                    final String topicNameAndPartition = dir.getName();
                    // 文件名的格式topic-partition
                    if (-1 == topicNameAndPartition.indexOf('-')) {
                        throw new IllegalArgumentException("error topic directory: " + dir.getAbsolutePath());
                    }

                    // key:topic,value:partition
                    final KV<String, Integer> topicPartion = Utils.getTopicPartition(topicNameAndPartition);
                    final String topic = topicPartion.k;
                    final int partition = topicPartion.v;
                    // 一个topic-partition文件夹对应一个Log
                    Log log = new Log(dir, partition, this.rollingStategy, flushInterval, needRecovery, maxMessageSize);

                    logs.putIfNotExists(topic, new Pool<Integer, Log>());
                    // key:partition,value:Log
                    Pool<Integer, Log> parts = logs.get(topic);
                    // 把当前分区号，添加到map
                    parts.put(partition, log);
                    // 获取这个topic分区的数目大小
                    int configPartition = getPartition(topic);
                    // 如果分区数目大于配置分区大小，则更新
                    if (configPartition <= partition) {
                        topicPartitionsMap.put(topic, partition + 1);
                    }
                }
            }
        }

        /* Schedule the cleanup task to delete old logs */
        // 启动清理日志调度器，异步清理日志
        if (this.scheduler != null) {
            logger.debug("starting log cleaner every " + logCleanupIntervalMs + " ms");
            this.scheduler.scheduleWithRate(new Runnable() {

                public void run() {
                    try {
                        cleanupLogs();
                    } catch (IOException e) {
                        logger.error("cleanup log failed.", e);
                    }
                }

            }, 60 * 1000, logCleanupIntervalMs);
        }

        // enable.zookeeper 是否启用zk来同步kafka日志的状态信息
        if (config.getEnableZookeeper()) {
            this.serverRegister = new ServerRegister(config, this);
            serverRegister.startup();
            // 将kafka的日志状态同步到zk，需要启动一个后台线程
            TopicRegisterTask task = new TopicRegisterTask();
            task.setName("jafka.topicregister");
            task.setDaemon(true);
            task.start();
        }
    }

    private void registeredTaskLooply() {
        while (!stopTopicRegisterTasks) {
            try {
                TopicTask task = topicRegisterTasks.take();
                if (task.type == TopicTask.TaskType.SHUTDOWN) break;
                serverRegister.processTask(task);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
        logger.debug("stop topic register task");
    }

    class TopicRegisterTask extends Thread {

        @Override
        public void run() {
            registeredTaskLooply();
        }
    }

    private Map<String, Long> getLogRetentionMSMap(Map<String, Integer> logRetentionHourMap) {
        Map<String, Long> ret = new HashMap<String, Long>();
        for (Map.Entry<String, Integer> e : logRetentionHourMap.entrySet()) {
            ret.put(e.getKey(), e.getValue() * 60 * 60 * 1000L);
        }
        return ret;
    }

    public void close() {
        logFlusherScheduler.shutdown();
        Iterator<Log> iter = getLogIterator();
        while (iter.hasNext()) {
            Closer.closeQuietly(iter.next(), logger);
        }
        if (config.getEnableZookeeper()) {
            stopTopicRegisterTasks = true;
            //wake up again and again
            topicRegisterTasks.add(new TopicTask(TopicTask.TaskType.SHUTDOWN, null));
            topicRegisterTasks.add(new TopicTask(TopicTask.TaskType.SHUTDOWN, null));
            Closer.closeQuietly(serverRegister);
        }
    }

    /**
     * Runs through the log removing segments older than a certain age
     *
     * @throws IOException
     */
    // 这个日志清理有两部分，清理过期的日志和清理超出配置日志大小的日志
    private void cleanupLogs() throws IOException {
        logger.trace("Beginning log cleanup...");
        int total = 0;
        Iterator<Log> iter = getLogIterator();
        long startMs = System.currentTimeMillis();
        while (iter.hasNext()) {
            Log log = iter.next();
            total += cleanupExpiredSegments(log) + cleanupSegmentsToMaintainSize(log);
        }
        if (total > 0) {
            logger.warn("Log cleanup completed. " + total + " files deleted in " + (System.currentTimeMillis() - startMs) / 1000 + " seconds");
        } else {
            logger.trace("Log cleanup completed. " + total + " files deleted in " + (System.currentTimeMillis() - startMs) / 1000 + " seconds");
        }
    }

    /**
     * Runs through the log removing segments until the size of the log is at least
     * logRetentionSize bytes in size
     *
     * @throws IOException
     */
    private int cleanupSegmentsToMaintainSize(final Log log) throws IOException {
        if (logRetentionSize < 0 || log.size() < logRetentionSize) return 0;

        List<LogSegment> toBeDeleted = log.markDeletedWhile(new LogSegmentFilter() {

            long diff = log.size() - logRetentionSize;

            // TODO ??????
            public boolean filter(LogSegment segment) {
                diff -= segment.size();
                return diff >= 0;
            }
        });
        return deleteSegments(log, toBeDeleted);
    }

    // 清理超过保存时期log.retention.hours的LogSegment
    private int cleanupExpiredSegments(Log log) throws IOException {
        final long startMs = System.currentTimeMillis();
        String topic = Utils.getTopicPartition(log.dir.getName()).k;
        Long logCleanupThresholdMS = logRetentionMSMap.get(topic);
        if (logCleanupThresholdMS == null) {
            logCleanupThresholdMS = this.logCleanupDefaultAgeMs;
        }
        final long expiredThrshold = logCleanupThresholdMS.longValue();
        // 先清理内存数据
        List<LogSegment> toBeDeleted = log.markDeletedWhile(new LogSegmentFilter() {

            public boolean filter(LogSegment segment) {
                //check file which has not been modified in expiredThrshold millionseconds
                return startMs - segment.getFile().lastModified() > expiredThrshold;
            }
        });
        return deleteSegments(log, toBeDeleted);
    }

    /**
     * Attemps to delete all provided segments from a log and returns how many it was able to
     */
    private int deleteSegments(Log log, List<LogSegment> segments) {
        int total = 0;
        for (LogSegment segment : segments) {
            boolean deleted = false;
            try {
                // 先清理内存数据，关闭FileChannel
                try {
                    segment.getMessageSet().close();
                } catch (IOException e) {
                    logger.warn(e.getMessage(), e);
                }
                // 然后删除磁盘上的文件
                if (!segment.getFile().delete()) {
                    deleted = true;
                } else {
                    total += 1;
                }
            } finally {
                logger.warn(String.format("DELETE_LOG[%s] %s => %s", log.name, segment.getFile().getAbsolutePath(),
                        deleted));
            }
        }
        return total;
    }

    /**
     * Register this broker in ZK for the first time.
     */
    public void startup() {
        // 如果启用zk了，则将注册当前broker到zk
        if (config.getEnableZookeeper()) {
            serverRegister.registerBrokerInZk();
            // 当前broker上所有的topic注册到 /broker/topics/topic-name/brokerId
            // data:为分区数目
            for (String topic : getAllTopics()) {
                serverRegister.processTask(new TopicTask(TopicTask.TaskType.CREATE, topic));
            }
            startupLatch.countDown();
        }
        // 启动刷出日志调度器，异步刷出内存数据到磁盘
        logger.debug("Starting log flusher every {} ms with the following overrides {}", config.getFlushSchedulerThreadRate(), logFlushIntervalMap);
        // log.default.flush.scheduler.interval.ms 刷出日志调度器时间间隔 默认1s
        // 注意这里并不是日志的刷出时间间隔，而是调度器
        logFlusherScheduler.scheduleWithRate(new Runnable() {

            public void run() {
                flushAllLogs(false);
            }
        }, config.getFlushSchedulerThreadRate(), config.getFlushSchedulerThreadRate());
    }

    /**
     * flush all messages to disk
     *
     * @param force flush anyway(ignore flush interval)
     */
    public void flushAllLogs(final boolean force) {
        Iterator<Log> iter = getLogIterator();
        while (iter.hasNext()) {
            Log log = iter.next();
            try {
                boolean needFlush = force;
                if (!needFlush) {
                    long timeSinceLastFlush = System.currentTimeMillis() - log.getLastFlushedTime();
                    Integer logFlushInterval = logFlushIntervalMap.get(log.getTopicName());
                    if (logFlushInterval == null) {
                        // log.default.flush.interval.ms 默认的日志刷出间隔
                        logFlushInterval = config.getDefaultFlushIntervalMs();
                    }
                    final String flushLogFormat = "[%s] flush interval %d, last flushed %d, need flush? %s";
                    needFlush = timeSinceLastFlush >= logFlushInterval.intValue();
                    logger.trace(String.format(flushLogFormat, log.getTopicName(), logFlushInterval,
                            log.getLastFlushedTime(), needFlush));
                }
                if (needFlush) {
                    // 刷出只是将最后一个LogSegment刷出
                    log.flush();
                }
            } catch (IOException ioe) {
                logger.error("Error flushing topic " + log.getTopicName(), ioe);
                logger.error("Halting due to unrecoverable I/O error while flushing logs: " + ioe.getMessage(), ioe);
                Runtime.getRuntime().halt(1);
            } catch (Exception e) {
                logger.error("Error flushing topic " + log.getTopicName(), e);
            }
        }
    }

    /**
     * 获取当前broker上所有的日志的topic
     *
     * @return
     */
    private Collection<String> getAllTopics() {
        return logs.keySet();
    }

    private Iterator<Log> getLogIterator() {
        return new IteratorTemplate<Log>() {

            final Iterator<Pool<Integer, Log>> iterator = logs.values().iterator();

            Iterator<Log> logIter;

            @Override
            protected Log makeNext() {
                while (true) {
                    if (logIter != null && logIter.hasNext()) {
                        return logIter.next();
                    }
                    if (!iterator.hasNext()) {
                        return allDone();
                    }
                    logIter = iterator.next().values().iterator();
                }
            }
        };
    }

    private void awaitStartup() {
        if (config.getEnableZookeeper()) {
            try {
                startupLatch.await();
            } catch (InterruptedException e) {
                logger.warn(e.getMessage(), e);
            }
        }
    }

    private Pool<Integer, Log> getLogPool(String topic, int partition) {
        awaitStartup();
        if (topic.length() <= 0) {
            throw new IllegalArgumentException("topic name can't be empty");
        }
        //
        Integer definePartition = this.topicPartitionsMap.get(topic);
        if (definePartition == null) {
            definePartition = numPartitions;
        }
        if (partition < 0 || partition >= definePartition.intValue()) {
            String msg = "Wrong partition [%d] for topic [%s], valid partitions [0,%d)";
            msg = format(msg, partition, topic, definePartition.intValue() - 1);
            logger.warn(msg);
            throw new InvalidPartitionException(msg);
        }
        return logs.get(topic);
    }

    /**
     * Get the log if exists or return null
     *
     * @param topic     topic name
     * @param partition partition index
     * @return a log for the topic or null if not exist
     */
    public ILog getLog(String topic, int partition) {
        TopicNameValidator.validate(topic);
        Pool<Integer, Log> p = getLogPool(topic, partition);
        return p == null ? null : p.get(partition);
    }

    /**
     * Create the log if it does not exist or return back exist log
     *
     * @param topic     the topic name
     * @param partition the partition id
     * @return read or create a log
     * @throws IOException any IOException
     */
    public ILog getOrCreateLog(String topic, int partition) throws IOException {
        final int configPartitionNumber = getPartition(topic);
        if (partition >= configPartitionNumber) {
            throw new IOException("partition is bigger than the number of configuration: " + configPartitionNumber);
        }
        boolean hasNewTopic = false;
        Pool<Integer, Log> parts = getLogPool(topic, partition);
        if (parts == null) {
            Pool<Integer, Log> found = logs.putIfNotExists(topic, new Pool<Integer, Log>());
            if (found == null) {
                hasNewTopic = true;
            }
            parts = logs.get(topic);
        }
        //
        Log log = parts.get(partition);
        if (log == null) {
            log = createLog(topic, partition);
            // 找到整个topic的Log,如果不存在说明Log还没有创建，也没有创建任何一个分区
            Log found = parts.putIfNotExists(partition, log);
            if (found != null) {
                Closer.closeQuietly(log, logger);
                log = found;
            } else {
                logger.info(format("Created log for [%s-%d], now create other logs if necessary", topic, partition));
                // 获取分区数目，创建所有的分区
                final int configPartitions = getPartition(topic);
                for (int i = 0; i < configPartitions; i++) {
                    // 递归创建分区
                    getOrCreateLog(topic, i);
                }
            }
        }
        if (hasNewTopic && config.getEnableZookeeper()) {
            topicRegisterTasks.add(new TopicTask(TopicTask.TaskType.CREATE, topic));
        }
        return log;
    }

    /**
     * create logs with given partition number
     * 更新topic的分区数目
     *
     * @param topic        the topic name
     * @param partitions   partition number
     * @param forceEnlarge enlarge the partition number of log if smaller than runtime
     * @return the partition number of the log after enlarging
     */
    public int createLogs(String topic, final int partitions, final boolean forceEnlarge) {
        TopicNameValidator.validate(topic);
        // 创建分区
        synchronized (logCreationLock) {
            // 获取当前topic的分区数目，如果没有则是默认的num.partitions 分区数目
            final int configPartitions = getPartition(topic);
            // 如果小于配置的分区大小，获取是非强制的，则直接返回分区数目
            if (configPartitions >= partitions || !forceEnlarge) {
                return configPartitions;
            }

            // 否则更新分区数目
            topicPartitionsMap.put(topic, partitions);
            // 如果启用了zk，则更新到zk
            if (config.getEnableZookeeper()) {
                if (getLogPool(topic, 0) != null) {//created already
                    topicRegisterTasks.add(new TopicTask(TopicTask.TaskType.ENLARGE, topic));
                } else {
                    topicRegisterTasks.add(new TopicTask(TopicTask.TaskType.CREATE, topic));
                }
            }
            return partitions;
        }
    }

    /**
     * delete topic who is never used
     * <p>
     * This will delete all log files and remove node data from zookeeper
     * </p>
     *
     * @param topic topic name
     * @param password auth password
     * @return number of deleted partitions or -1 if authentication failed
     */
    public int deleteLogs(String topic, String password) {
        if (!config.getAuthentication().auth(password)) {
            return -1;
        }
        int value = 0;
        synchronized (logCreationLock) {
            Pool<Integer, Log> parts = logs.remove(topic);
            if (parts != null) {
                List<Log> deleteLogs = new ArrayList<Log>(parts.values());
                for (Log log : deleteLogs) {
                    log.delete();
                    value++;
                }
            }
            if (config.getEnableZookeeper()) {
                topicRegisterTasks.add(new TopicTask(TopicTask.TaskType.DELETE, topic));
            }
        }
        return value;
    }

    /**
     * 创建分区目录
     *
     * @param topic
     * @param partition
     * @return
     * @throws IOException
     */
    private Log createLog(String topic, int partition) throws IOException {
        synchronized (logCreationLock) {
            File d = new File(logDir, topic + "-" + partition);
            d.mkdirs();
            return new Log(d, partition, this.rollingStategy, flushInterval, false, maxMessageSize);
        }
    }

    private int getPartition(String topic) {
        Integer p = topicPartitionsMap.get(topic);
        return p != null ? p.intValue() : this.numPartitions;
    }

    /**
     * Pick a random partition from the given topic
     */
    public int choosePartition(String topic) {
        return random.nextInt(getPartition(topic));
    }

    /**
     * read offsets before given time
     *
     * @param offsetRequest the offset request
     * @return offsets before given time
     */
    public List<Long> getOffsets(OffsetRequest offsetRequest) {
        // 先获取指定topic,指定partition的Log
        ILog log = getLog(offsetRequest.topic, offsetRequest.partition);
        if (log != null) {
            return log.getOffsetsBefore(offsetRequest);
        }
        return ILog.EMPTY_OFFSETS;
    }

    public Map<String, Integer> getTopicPartitionsMap() {
        return topicPartitionsMap;
    }

}
