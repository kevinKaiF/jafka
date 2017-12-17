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


import io.jafka.api.OffsetRequest;
import io.jafka.common.ErrorMapping;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.GatheringByteChannel;
import java.util.List;


/**
 * @author adyliu (imxylz@gmail.com)
 * @since 1.0
 */
public class OffsetArraySend extends AbstractSend {

   final ByteBuffer header = ByteBuffer.allocate(6);
   final ByteBuffer contentBuffer;
   public OffsetArraySend(List<Long> offsets) {
       // 4个字节存储，offsets的size
       // offset.size() * 8,每个offset都是Long类型，需要8个字节
       // 最后2个字节，存储ErrorMapping，即响应状态码
       header.putInt(4 + offsets.size()*8 +2);
       header.putShort(ErrorMapping.NoError.code);
       header.rewind();
       contentBuffer = OffsetRequest.serializeOffsetArray(offsets);
   }
    public int writeTo(GatheringByteChannel channel) throws IOException {
        expectIncomplete();
        int written = 0;
        // header还没有写入，那么将header写入到channel
        if(header.hasRemaining()) {
            written += channel.write(header);
        }
        // 如果header已经写完，contentBuffer还没有写完，则将contentBuffer写出到channel
        if(!header.hasRemaining() && contentBuffer.hasRemaining()) {
            written += channel.write(contentBuffer);
        }
        // 如果contentBuffer写出完毕，则设置标志为已完成
        if(!contentBuffer.hasRemaining()) {
            setCompleted();
        }
        return written;
    }

}
