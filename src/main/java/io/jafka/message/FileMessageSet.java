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

package io.jafka.message;

import io.jafka.mx.LogFlushStats;
import io.jafka.utils.IteratorTemplate;
import io.jafka.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.GatheringByteChannel;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * An on-disk message set. The set can be opened either mutably or immutably. Mutation attempts
 * will fail on an immutable message set. An optional limit and offset can be applied to the
 * message set which will control the offset into the file and the effective length into the
 * file from which messages will be read
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class FileMessageSet extends MessageSet {

    private final Logger logger = LoggerFactory.getLogger(FileMessageSet.class);

    private final FileChannel channel;

    // 这个offset是文件读取字节的偏移量
    private final long offset;

    private final boolean mutable;

    private final AtomicBoolean needRecover;

    /////////////////////////////////////////////////////////////////////////
    private final AtomicLong setSize = new AtomicLong();

    private final AtomicLong setHighWaterMark = new AtomicLong();

    public FileMessageSet(FileChannel channel, long offset, long limit, //
                          boolean mutable, AtomicBoolean needRecover) throws IOException {
        super();
        this.channel = channel;
        // 这个offset是当前日志文件的字节偏移量
        this.offset = offset;
        this.mutable = mutable;
        this.needRecover = needRecover;
        if (mutable) {
            if (limit < Long.MAX_VALUE || offset > 0) throw new IllegalArgumentException(
                    "Attempt to open a mutable message set with a view or offset, which is not allowed.");

            if (needRecover.get()) {
                // set the file position to the end of the file for appending messages
                long startMs = System.currentTimeMillis();
                long truncated = recover();
                logger.info("Recovery succeeded in " + (System.currentTimeMillis() - startMs) / 1000 + " seconds. " + truncated + " bytes truncated.");
            } else {
                setSize.set(channel.size());
                setHighWaterMark.set(getSizeInBytes());
                channel.position(channel.size());
            }
        } else {
            setSize.set(Math.min(channel.size(), limit) - offset);
            setHighWaterMark.set(getSizeInBytes());
        }
    }

    /**
     * Create a file message set with no limit or offset
     * @param channel file channel
     * @param mutable file writeable
     * @throws IOException any file exception
     */
    public FileMessageSet(FileChannel channel, boolean mutable) throws IOException {
        this(channel, 0, Long.MAX_VALUE, mutable, new AtomicBoolean(false));
    }

    /**
     * Create a file message set with no limit or offset
     * @param file to store message
     * @param mutable file writeable
     * @throws IOException any file exception
     */
    public FileMessageSet(File file, boolean mutable) throws IOException {
        this(Utils.openChannel(file, mutable), mutable);
    }

    /**
     * Create a file message set with no limit or offset
     * @param channel file channel
     * @param mutable file writeable
     * @param needRecover check the file on boost
     * @throws IOException any file exception
     */
    public FileMessageSet(FileChannel channel, boolean mutable, AtomicBoolean needRecover) throws IOException {
        this(channel, 0, Long.MAX_VALUE, mutable, needRecover);
    }

    /**
     * Create a file message set with no limit or offset
     * @param file file to write
     * @param mutable file writeable
     * @param needRecover check the file on boost
     * @throws IOException any file exception
     */
    public FileMessageSet(File file, boolean mutable, AtomicBoolean needRecover) throws IOException {
        this(Utils.openChannel(file, mutable), mutable, needRecover);
    }

    public Iterator<MessageAndOffset> iterator() {
        return new IteratorTemplate<MessageAndOffset>() {

            long location = offset;

            @Override
            protected MessageAndOffset makeNext() {
                try {
                    // 获取4个字节的message大小值
                    ByteBuffer sizeBuffer = ByteBuffer.allocate(4);
                    channel.read(sizeBuffer, location);
                    // 理应读到4个字节，此时的position == limit == 4
                    // 如果ByteBuffer还有剩余空间，说明读取是有问题的
                    if (sizeBuffer.hasRemaining()) {
                        return allDone();
                    }
                    sizeBuffer.rewind();
                    // 这个大小不仅包含6个字节的首部，还有N-6个字节的payload
                    int size = sizeBuffer.getInt();
                    // 1个字节 magic
                    // 1个字节 attributes
                    // 4个字节 crc32
                    // 不能少于6个字节
                    if (size < Message.MinHeaderSize) {
                        return allDone();
                    }
                    ByteBuffer buffer = ByteBuffer.allocate(size);
                    channel.read(buffer, location + 4);
                    if (buffer.hasRemaining()) {
                        return allDone();
                    }
                    buffer.rewind();
                    location += size + 4;

                    return new MessageAndOffset(new Message(buffer), location);
                } catch (IOException e) {
                    throw new RuntimeException(e.getMessage(), e);
                }
            }
        };
    }

    /**
     * the max offset(next message id).<br>
     * The <code> #getSizeInBytes()</code> maybe is larger than {@link #highWaterMark()}
     * while some messages were cached in memory(not flush to disk).
     */
    public long getSizeInBytes() {
        return setSize.get();
    }

    @Override
    public long writeTo(GatheringByteChannel destChannel, long writeOffset, long maxSize) throws IOException {
        return channel.transferTo(offset + writeOffset, Math.min(maxSize, getSizeInBytes()), destChannel);
    }

    /**
     * read message from file
     *
     * @param readOffset offset in this channel(file);not the message offset
     * @param size       max data size
     * @return messages sharding data with file log
     * @throws IOException reading file failed
     */
    // 这个readOffset是文件的offset,不是message的offset
    public MessageSet read(long readOffset, long size) throws IOException {
        // 共享channel
        return new FileMessageSet(channel, this.offset + readOffset,
                Math.min(this.offset + readOffset + size, highWaterMark()), false, new AtomicBoolean(false));
    }

    /**
     * Append this message to the message set
     * @param messages message to append
     * @return the written size and first offset
     * @throws IOException file write exception
     */
    public long[] append(MessageSet messages) throws IOException {
        checkMutable();
        long written = 0L;
        while (written < messages.getSizeInBytes())
            written += messages.writeTo(channel, 0, messages.getSizeInBytes());
        long beforeOffset = setSize.getAndAdd(written);
        return new long[]{written, beforeOffset};
    }

    /**
     * Commit all written data to the physical disk
     *
     * @throws IOException any io exception
     */
    public void flush() throws IOException {
        checkMutable();
        long startTime = System.currentTimeMillis();
        channel.force(true);
        long elapsedTime = System.currentTimeMillis() - startTime;
        LogFlushStats.recordFlushRequest(elapsedTime);
        logger.debug("flush time " + elapsedTime);
        setHighWaterMark.set(getSizeInBytes());
        logger.debug("flush high water mark:" + highWaterMark());
    }

    /**
     * Close this message set
     *
     * @throws IOException file close exception
     */
    public void close() throws IOException {
        if (mutable) flush();
        channel.close();
    }

    /**
     * Recover log up to the last complete entry. Truncate off any bytes from any incomplete
     * messages written
     *
     * @return truncate掉的字节数
     * @throws IOException any exception
     */
    private long recover() throws IOException {
        checkMutable();
        long len = channel.size();
        ByteBuffer buffer = ByteBuffer.allocate(4);
        long validUpTo = 0;
        long next = 0L;
        do {
            next = validateMessage(channel, validUpTo, len, buffer);
            if (next >= 0) validUpTo = next;
        } while (next >= 0);
        // 将validUpTo之后的byte删除
        channel.truncate(validUpTo);
        // 更新文件大小
        setSize.set(validUpTo);
        setHighWaterMark.set(validUpTo);
        logger.info("recover high water mark:" + highWaterMark());
        /* This should not be necessary, but fixes bug 6191269 on some OSs. */
        channel.position(validUpTo);
        needRecover.set(false);
        return len - validUpTo;
    }

    /**
     * Read, validate, and discard a single message, returning the next valid offset, and the
     * message being validated
     *
     * @throws IOException any exception
     */
    private long validateMessage(FileChannel channel, long start, long len, ByteBuffer buffer) throws IOException {
        buffer.rewind();
        // 获取message的大小
        int read = channel.read(buffer, start);
        if (read < 4) return -1;

        // check that we have sufficient bytes left in the file
        // getInt(int index)是从绝对位置开始读取，与当前的position无关，position不会变化
        // getInt()是从相对位置开始读取，从当前的position读取，而且position += 4移动到下个位置
        // 这个size，不仅有6个字节的头部，还有N-6个字节的payload
        int size = buffer.getInt(0);
        // 如果小于头部字节个数，直接返回
        if (size < Message.MinHeaderSize) return -1;

        // 最新的读取位置，start是偏移量，4个字节是需要读取的message length大小，size是message数据大小
        long next = start + 4 + size;
        if (next > len) return -1;

        // read the message
        ByteBuffer messageBuffer = ByteBuffer.allocate(size);
        long curr = start + 4;
        // 一直读完
        while (messageBuffer.hasRemaining()) {
            read = channel.read(messageBuffer, curr);
            // 如果没读到，说明文件发生了更改，按道理是肯定能读到数据的，因为messageBuffer大小是按其真实的size分配的
            if (read < 0) throw new IllegalStateException("File size changed during recovery!");
            // 位置累加
            else curr += read;
        }
        // 需要重置position
        messageBuffer.rewind();
        Message message = new Message(messageBuffer);
        // 校验crc32
        if (!message.isValid()) return -1;
        else return next;
    }

    void checkMutable() {
        if (!mutable) throw new IllegalStateException("Attempt to invoke mutation on immutable message set.");
    }

    /**
     * The max offset(next message id) persisted in the log file.<br>
     * Messages with smaller offsets have persisted in file.
     *
     * @return max offset
     */
    public long highWaterMark() {
        return setHighWaterMark.get();
    }

}
