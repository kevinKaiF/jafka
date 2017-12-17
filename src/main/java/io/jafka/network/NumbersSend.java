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

package io.jafka.network;

import io.jafka.common.ErrorMapping;
import io.jafka.utils.Utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;

/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.1
 */
public abstract class NumbersSend extends AbstractSend {

    protected ByteBuffer header = ByteBuffer.allocate(6);

    protected ByteBuffer contentBuffer;

    NumbersSend() {
    }

    public int writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        // 因为之前已经rewind过
        // header的数目如果还没有写出，则写出到channel
        if (header.hasRemaining()) {
            written += channel.write(header);
        }

        // 因为之前已经rewind过
        // 如果contentBuffer还没有写出，则写出到channel
        if (!header.hasRemaining() && contentBuffer.hasRemaining()) {
            written += channel.write(contentBuffer);
        }
        // 如果contentBuffer已经写满，则设置为已完成
        if (!contentBuffer.hasRemaining()) {
            setCompleted();
        }
        return written;
    }

    public static class IntegersSend extends NumbersSend {

        public IntegersSend(int... numbers) {
            // 4个字节存储  numbers的length
            // 4 * numbers.length 是每个number都是int
            // 最后2个字节存储ErrorMapping
            header.putInt(4 + numbers.length * 4 + 2);
            header.putShort(ErrorMapping.NoError.code);
            header.rewind();
            contentBuffer = Utils.serializeArray(numbers);
        }
    }

    public static class LongsSend extends NumbersSend {

        public LongsSend(long... numbers) {
            header.putInt(4 + numbers.length * 8 + 2);
            header.putShort(ErrorMapping.NoError.code);
            header.rewind();
            contentBuffer = Utils.serializeArray(numbers);
        }
    }
}
