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

package io.jafka.api;

import io.jafka.common.annotations.ClientSide;
import io.jafka.common.annotations.ServerSide;
import io.jafka.network.Request;
import io.jafka.utils.Utils;

import java.nio.ByteBuffer;

/**
 * Create Operation
 * <p>
 * This operation creates topic in a broker or enlarge the partition number of topic.
 *
 * @author adyliu (imxylz@gmail.com)
 * @since 1.2
 */
@ClientSide
@ServerSide
public class CreaterRequest implements Request {

    private static final byte FORCE_ENLARGE = (byte) 1;

    private static final byte IGNORE_ENLARGE = (byte) 0;

    /**
     * topic name
     */
    public final String topic;

    /**
     * topic partition will be created or enlarged
     */
    public final int partitions;

    /**
     * enlarge the partition number if {@link #partitions} is larger than the real number.
     */
    public final boolean enlarge;

    public CreaterRequest(String topic, int partitions) {
        this(topic, partitions, false);
    }

    public CreaterRequest(String topic, int partitions, boolean enlarge) {
        this.topic = topic;
        this.partitions = partitions;
        this.enlarge = enlarge;
        if (partitions < 0) {
            throw new IllegalArgumentException("partitions must be non-negative number");
        }
    }

    @Override
    public int getSizeInBytes() {
        return Utils.caculateShortString(topic) + 4 + 1;
    }

    @Override
    public RequestKeys getRequestKey() {
        return RequestKeys.CREATE;
    }

    @Override
    public void writeTo(ByteBuffer buffer) {
        Utils.writeShortString(buffer, topic);
        buffer.putInt(partitions);
        buffer.put(enlarge ? FORCE_ENLARGE : IGNORE_ENLARGE);
    }

    @Override
    public String toString() {
        return String.format("CreateRequest [topic=%s, partitions=%s, enlarge=%s]", topic, partitions, enlarge);
    }

    /**
     * 数据格式
     * 2-topic.size     |short          |4          |1              |
     * topic-size       |topic          |partition  |forceEnlarge   |
     *
     * forceEnlarge是分区扩容标志
     *
     * @param buffer
     * @return
     */
    public static CreaterRequest readFrom(ByteBuffer buffer) {
        String topic = Utils.readShortString(buffer);
        int partitions = buffer.getInt();
        boolean enlarge = buffer.get() == FORCE_ENLARGE;
        return new CreaterRequest(topic, partitions, enlarge);
    }
}
