/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.common.message;

import com.alibaba.fastjson.JSON;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.junit.Test;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.Charset;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

public class MessageDecoderTest {

    @Test
    public void testDecodeProperties() {
        MessageExt messageExt = new MessageExt();

        messageExt.setMsgId("645100FA00002A9F000000489A3AA09E");
        messageExt.setTopic("abc");
        messageExt.setBody("hello!q!".getBytes());
        try {
            messageExt.setBornHost(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }
        messageExt.setBornTimestamp(System.currentTimeMillis());
        messageExt.setCommitLogOffset(123456);
        messageExt.setPreparedTransactionOffset(0);
        messageExt.setQueueId(0);
        messageExt.setQueueOffset(123);
        messageExt.setReconsumeTimes(0);
        try {
            messageExt.setStoreHost(new InetSocketAddress(InetAddress.getLocalHost(), 0));
        } catch (UnknownHostException e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        messageExt.putUserProperty("a", "123");
        messageExt.putUserProperty("b", "hello");
        messageExt.putUserProperty("c", "3.14");

        byte[] msgBytes = new byte[0];
        try {
            msgBytes = MessageDecoder.encode(messageExt, false);
        } catch (Exception e) {
            e.printStackTrace();
            assertThat(Boolean.FALSE).isTrue();
        }

        ByteBuffer byteBuffer = ByteBuffer.allocate(msgBytes.length);
        byteBuffer.put(msgBytes);

        Map<String, String> properties = MessageDecoder.decodeProperties(byteBuffer);

        assertThat(properties).isNotNull();
        assertThat("123").isEqualTo(properties.get("a"));
        assertThat("hello").isEqualTo(properties.get("b"));
        assertThat("3.14").isEqualTo(properties.get("c"));
    }

    @Test
    public void testResolveCommitLog() throws Exception {
        String path = "/Users/lixiongcheng/store/commitlog/00000000000000000000";
        RandomAccessFile randomAccessFile = new RandomAccessFile(new File(path), "rw");
        FileChannel channel = randomAccessFile.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024 * 1024);
        channel.read(buffer);
        buffer.flip();

        //开始读取数据
        int nextPos = 0;

        while (buffer.remaining() > 0) {
            int totalSize = buffer.getInt();
            System.out.println(totalSize);

            buffer.position(nextPos);


            byte[] sByte = new byte[totalSize];
            buffer.get(sByte);
            nextPos = buffer.position();

            ByteBuffer newByteBuffer = ByteBuffer.wrap(sByte);

            MessageExt messageExt = MessageDecoder.decode(newByteBuffer);
            if (messageExt == null) {
                break;
            }
            System.out.println(JSON.toJSONString(messageExt));
        }
    }


    @Test
    public void testResolveCommitLog2() throws Exception {
        String path = "/Users/lixiongcheng/store/commitlog/00000000000000000000";
        RandomAccessFile randomAccessFile = new RandomAccessFile(new File(path), "rw");
        FileChannel channel = randomAccessFile.getChannel();
        ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024 * 1024);
        channel.read(buffer);
        buffer.flip();

        //开始读取数据
        int nextPos = 0;

        while (buffer.remaining() > 0) {
            int totalSize = buffer.getInt();
            if(totalSize == 0){
                break;
            }
            buffer.position(nextPos);


            byte[] sByte = new byte[totalSize];
            buffer.get(sByte);
            nextPos = buffer.position();

            ByteBuffer newByteBuffer = ByteBuffer.wrap(sByte);

            decode(newByteBuffer);
            System.out.println("====================================");
        }
    }



    public static MessageExt decode(ByteBuffer byteBuffer) {
        try {
            // 1 TOTALSIZE
            int storeSize = byteBuffer.getInt();
            System.out.println("storeSize---->" + storeSize);

            // 2 MAGICCODE
            int magic = byteBuffer.getInt();
            System.out.println("magic---->" + magic);

            // 3 BODYCRC
            int bodyCRC = byteBuffer.getInt();
            System.out.println("bodyCRC---->" + bodyCRC);

            // 4 QUEUEID
            int queueId = byteBuffer.getInt();
            System.out.println("queueId---->" + queueId);

            // 5 FLAG
            int flag = byteBuffer.getInt();
            System.out.println("flag---->" + flag);

            // 6 QUEUEOFFSET
            long queueOffset = byteBuffer.getLong();
            System.out.println("queueOffset---->" + queueOffset);

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();
            System.out.println("physicOffset---->" + physicOffset);

            // 8 SYSFLAG
            int sysFlag = byteBuffer.getInt();
            System.out.println("sysFlag---->" + sysFlag);

            // 9 BORNTIMESTAMP
            long bornTimeStamp = byteBuffer.getLong();
            System.out.println("bornTimeStamp---->" + bornTimeStamp);

            // 10 BORNHOST
            byte[] bornHost = new byte[4];
            byteBuffer.get(bornHost, 0, 4);
            int port = byteBuffer.getInt();
            System.out.println("bornHost.host---->" + InetAddress.getByAddress(bornHost));
            System.out.println("bornHost.ip---->" + port);

            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuffer.getLong();
            System.out.println("storeTimestamp---->" + storeTimestamp);

            // 12 STOREHOST
            byte[] storeHost = new byte[4];
            ByteBuffer storeHostBuf = byteBuffer.get(storeHost, 0, 4);
            port = byteBuffer.getInt();
            System.out.println("storeHost.host---->" + InetAddress.getByAddress(storeHost));
            System.out.println("storeHost.ip---->" + port);

            // 13 RECONSUMETIMES
            int reconsumeTimes = byteBuffer.getInt();
            System.out.println("reconsumeTimes---->" + reconsumeTimes);

            // 14 Prepared Transaction Offset
            long preparedTransactionOffset = byteBuffer.getLong();
            System.out.println("preparedTransactionOffset---->" + preparedTransactionOffset);

            // 15 BODY
            int bodyLen = byteBuffer.getInt();

            byte[] body = new byte[bodyLen];
            byteBuffer.get(body);
            System.out.println("bodyLen---->" + bodyLen);
            System.out.println("body---->" + new String(body, Charset.defaultCharset()));

            // 16 TOPIC
            byte topicLen = byteBuffer.get();
            byte[] topic = new byte[(int) topicLen];
            byteBuffer.get(topic);
            System.out.println("topicLen---->" + topicLen);
            System.out.println("topic---->" + new String(topic,Charset.defaultCharset()));

            // 17 properties
            short propertiesLength = byteBuffer.getShort();
            byte[] properties = new byte[propertiesLength];
            byteBuffer.get(properties);
            System.out.println("propertiesLength---->" + propertiesLength);
            System.out.println("properties---->" + new String(properties,Charset.defaultCharset()));

            ByteBuffer byteBufferMsgId = ByteBuffer.allocate(16);
            String msgId = MessageDecoder.createMessageId(byteBufferMsgId, storeHostBuf, physicOffset);
            System.out.println("msgId---->" + msgId);
        } catch (Exception e) {
            byteBuffer.position(byteBuffer.limit());
        }

        return null;
    }

}
