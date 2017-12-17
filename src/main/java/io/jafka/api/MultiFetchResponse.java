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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import io.jafka.common.ErrorMapping;
import io.jafka.message.ByteBufferMessageSet;

/**
 * a response with mulit-data
 * 
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class MultiFetchResponse implements Iterable<ByteBufferMessageSet> {

    private final List<ByteBufferMessageSet> messageSets;

    /**
     * create a multi-response
     * <p>
     * buffer format: <b> size+errorCode(short)+payload+size+errorCode(short)+payload+... </b>
     * <p>
     * size = 2(short)+length(payload)
     * 
     * @param buffer the whole data buffer
     * @param numSets response count
     * @param offsets message offset for each response
     */
    // numSets表示对同一个broker, 抓取多个partition的个数
    // offsets的都是有顺序的，不能错
    public MultiFetchResponse(ByteBuffer buffer, int numSets, List<Long> offsets) {
        super();
        this.messageSets = new ArrayList<ByteBufferMessageSet>();
        for (int i = 0; i < numSets; i++) {
            // 4个字节   这个数据体的大小
            int size = buffer.getInt();
            // 2个字节   错误编号
            short errorCode = buffer.getShort();
            // 获取真正的数据体
            ByteBuffer copy = buffer.slice();
            // 除去2个字节的 errorCode
            int payloadSize = size - 2;
            // 重置limit,限定需要读取的数据
            copy.limit(payloadSize);
            //move position for next reading
            // 重置buffer的位置
            buffer.position(buffer.position() + payloadSize);
            // 转为ByteBufferMessageSet
            messageSets.add(new ByteBufferMessageSet(copy, offsets.get(i), ErrorMapping.valueOf(errorCode)));
        }
    }

    public Iterator<ByteBufferMessageSet> iterator() {
        return messageSets.iterator();
    }

    public int size() {
        return messageSets.size();
    }
    
    public boolean isEmpty() {
        return size() == 0;
    }
}
