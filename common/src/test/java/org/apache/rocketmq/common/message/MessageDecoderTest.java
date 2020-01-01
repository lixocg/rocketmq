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
            if (totalSize == 0) {
                break;
            }
            buffer.position(nextPos);


            byte[] sByte = new byte[totalSize];
            buffer.get(sByte);
            nextPos = buffer.position();

            ByteBuffer newByteBuffer = ByteBuffer.wrap(sByte);

            MyMessage myMessage = decode(newByteBuffer);
            System.out.println(myMessage);
        }
    }

    @Test
    public void testResolveConsumeQueue() throws Exception {
        String topic = "MyTopic";
        for (int queueId = 0; queueId < 4; queueId++) {
            String path = "/Users/lixiongcheng/store/consumequeue/MyTopic/" + queueId + "/00000000000000000000";
            RandomAccessFile randomAccessFile = new RandomAccessFile(new File(path), "rw");
            FileChannel channel = randomAccessFile.getChannel();
            ByteBuffer buffer = ByteBuffer.allocateDirect(1024 * 1024 * 1024);
            channel.read(buffer);
            buffer.flip();

            //开始读取数据
            int nextPos = 0;


            while (buffer.remaining() > 0) {
                long commitLogOffset = buffer.getLong();
                int size = buffer.getInt();
                long tagHash = buffer.getLong();
                if (size == 0) {
                    break;
                }
                nextPos += 20;
                buffer.position(nextPos);

                ConsumeQueue consumeQueue = new ConsumeQueue();
                consumeQueue.setTopic(topic);
                consumeQueue.setQueueId(queueId);
                consumeQueue.setCommitLogOffset(commitLogOffset);
                consumeQueue.setSize(size);
                consumeQueue.setTagHashcode(tagHash);
                System.out.println(consumeQueue);
            }
            System.out.println("=========");
        }


    }

    static class ConsumeQueue {
        private String topic;
        private int queueId;
        private long commitLogOffset;
        private int size;
        private long tagHashcode;

        @Override
        public String toString() {
            return "ConsumeQueue{" +
                    "topic='" + topic + '\'' +
                    ", queueId=" + queueId +
                    ", commitLogOffset=" + commitLogOffset +
                    ", size=" + size +
                    ", tagHashcode=" + tagHashcode +
                    '}';
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public int getQueueId() {
            return queueId;
        }

        public void setQueueId(int queueId) {
            this.queueId = queueId;
        }

        public long getCommitLogOffset() {
            return commitLogOffset;
        }

        public void setCommitLogOffset(long commitLogOffset) {
            this.commitLogOffset = commitLogOffset;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            this.size = size;
        }

        public long getTagHashcode() {
            return tagHashcode;
        }

        public void setTagHashcode(long tagHashcode) {
            this.tagHashcode = tagHashcode;
        }
    }


    static class MyMessage {
        private int storeSize;//1
        private int magic;//2
        private int bodyCrc;//3
        private int queueId;//4
        private int flag;//5
        private long queueOffset;//6
        private long physicalOffset;//7
        private int sysFlag;//8
        private long bornTimestamp;//9
        private String bornHost;//10
        private int bornPort;//10
        private long storeTimestamp;//11
        private String storeHost;//12
        private int storeIp;//12
        private int reconsumeTimes;//13
        private long preparedTransactionOffset;//14
        private int bodyLen;//15
        private String body;//15
        private int topicLen;//16
        private String topic;//16
        private short propertiesLength;//17
        private String properties;//17
        private String msgId;

        @Override
        public String toString() {
            return "MyMessage{" +
                    "storeSize=" + storeSize +
                    ", magic=" + magic +
                    ", bodyCrc=" + bodyCrc +
                    ", queueId=" + queueId +
                    ", flag=" + flag +
                    ", queueOffset=" + queueOffset +
                    ", physicalOffset=" + physicalOffset +
                    ", sysFlag=" + sysFlag +
                    ", bornTimestamp=" + bornTimestamp +
                    ", bornHost='" + bornHost + '\'' +
                    ", bornPort=" + bornPort +
                    ", storeTimestamp=" + storeTimestamp +
                    ", storeHost='" + storeHost + '\'' +
                    ", storeIp=" + storeIp +
                    ", reconsumeTimes=" + reconsumeTimes +
                    ", preparedTransactionOffset=" + preparedTransactionOffset +
                    ", bodyLen=" + bodyLen +
                    ", body='" + body + '\'' +
                    ", topicLen=" + topicLen +
                    ", topic='" + topic + '\'' +
                    ", propertiesLength=" + propertiesLength +
                    ", properties='" + properties + '\'' +
                    ", msgId='" + msgId + '\'' +
                    '}';
        }

        public int getMagic() {
            return magic;
        }

        public void setMagic(int magic) {
            this.magic = magic;
        }

        public int getBodyCrc() {
            return bodyCrc;
        }

        public void setBodyCrc(int bodyCrc) {
            this.bodyCrc = bodyCrc;
        }

        public int getQueueId() {
            return queueId;
        }

        public void setQueueId(int queueId) {
            this.queueId = queueId;
        }

        public int getFlag() {
            return flag;
        }

        public void setFlag(int flag) {
            this.flag = flag;
        }

        public long getQueueOffset() {
            return queueOffset;
        }

        public void setQueueOffset(long queueOffset) {
            this.queueOffset = queueOffset;
        }

        public long getPhysicalOffset() {
            return physicalOffset;
        }

        public void setPhysicalOffset(long physicalOffset) {
            this.physicalOffset = physicalOffset;
        }

        public int getSysFlag() {
            return sysFlag;
        }

        public void setSysFlag(int sysFlag) {
            this.sysFlag = sysFlag;
        }

        public long getBornTimestamp() {
            return bornTimestamp;
        }

        public void setBornTimestamp(long bornTimestamp) {
            this.bornTimestamp = bornTimestamp;
        }

        public String getBornHost() {
            return bornHost;
        }

        public void setBornHost(String bornHost) {
            this.bornHost = bornHost;
        }

        public int getBornPort() {
            return bornPort;
        }

        public void setBornPort(int bornPort) {
            this.bornPort = bornPort;
        }

        public long getStoreTimestamp() {
            return storeTimestamp;
        }

        public void setStoreTimestamp(long storeTimestamp) {
            this.storeTimestamp = storeTimestamp;
        }

        public String getStoreHost() {
            return storeHost;
        }

        public void setStoreHost(String storeHost) {
            this.storeHost = storeHost;
        }

        public int getStoreIp() {
            return storeIp;
        }

        public void setStoreIp(int storeIp) {
            this.storeIp = storeIp;
        }

        public long getPreparedTransactionOffset() {
            return preparedTransactionOffset;
        }

        public void setPreparedTransactionOffset(long preparedTransactionOffset) {
            this.preparedTransactionOffset = preparedTransactionOffset;
        }

        public int getBodyLen() {
            return bodyLen;
        }

        public void setBodyLen(int bodyLen) {
            this.bodyLen = bodyLen;
        }

        public String getBody() {
            return body;
        }

        public void setBody(String body) {
            this.body = body;
        }

        public int getTopicLen() {
            return topicLen;
        }

        public void setTopicLen(int topicLen) {
            this.topicLen = topicLen;
        }

        public String getTopic() {
            return topic;
        }

        public void setTopic(String topic) {
            this.topic = topic;
        }

        public short getPropertiesLength() {
            return propertiesLength;
        }

        public void setPropertiesLength(short propertiesLength) {
            this.propertiesLength = propertiesLength;
        }

        public String getProperties() {
            return properties;
        }

        public void setProperties(String properties) {
            this.properties = properties;
        }

        public String getMsgId() {
            return msgId;
        }

        public void setMsgId(String msgId) {
            this.msgId = msgId;
        }

        public int getReconsumeTimes() {
            return reconsumeTimes;
        }

        public void setReconsumeTimes(int reconsumeTimes) {
            this.reconsumeTimes = reconsumeTimes;
        }

        public int getStoreSize() {
            return storeSize;
        }

        public void setStoreSize(int storeSize) {
            this.storeSize = storeSize;
        }
    }

    public static MyMessage decode(ByteBuffer byteBuffer) {
        try {
            MyMessage message = new MyMessage();
            // 1 TOTALSIZE
            int storeSize = byteBuffer.getInt();
            message.setStoreSize(storeSize);

            // 2 MAGICCODE
            int magic = byteBuffer.getInt();
            message.setMagic(magic);

            // 3 BODYCRC
            int bodyCRC = byteBuffer.getInt();
            message.setBodyCrc(bodyCRC);

            // 4 QUEUEID
            int queueId = byteBuffer.getInt();
            message.setQueueId(queueId);

            // 5 FLAG
            int flag = byteBuffer.getInt();
            message.setFlag(flag);

            // 6 QUEUEOFFSET
            long queueOffset = byteBuffer.getLong();
            message.setQueueOffset(queueOffset);

            // 7 PHYSICALOFFSET
            long physicOffset = byteBuffer.getLong();
            message.setPhysicalOffset(physicOffset);

            // 8 SYSFLAG
            int sysFlag = byteBuffer.getInt();
            message.setSysFlag(sysFlag);

            // 9 BORNTIMESTAMP
            long bornTimeStamp = byteBuffer.getLong();
            message.setBornTimestamp(bornTimeStamp);

            // 10 BORNHOST
            byte[] bornHost = new byte[4];
            byteBuffer.get(bornHost, 0, 4);
            int port = byteBuffer.getInt();
            message.setBornHost(InetAddress.getByAddress(bornHost).toString());
            message.setBornPort(port);

            // 11 STORETIMESTAMP
            long storeTimestamp = byteBuffer.getLong();
            message.setStoreTimestamp(storeTimestamp);

            // 12 STOREHOST
            byte[] storeHost = new byte[4];
            ByteBuffer storeHostBuf = byteBuffer.get(storeHost, 0, 4);
            port = byteBuffer.getInt();
            message.setStoreHost(InetAddress.getByAddress(storeHost).toString());
            message.setStoreIp(port);

            // 13 RECONSUMETIMES
            int reconsumeTimes = byteBuffer.getInt();
            message.setReconsumeTimes(reconsumeTimes);

            // 14 Prepared Transaction Offset
            long preparedTransactionOffset = byteBuffer.getLong();
            message.setPreparedTransactionOffset(preparedTransactionOffset);

            // 15 BODY
            int bodyLen = byteBuffer.getInt();

            byte[] body = new byte[bodyLen];
            byteBuffer.get(body);
            message.setBodyLen(bodyLen);
            message.setBody(new String(body, Charset.defaultCharset()));

            // 16 TOPIC
            byte topicLen = byteBuffer.get();
            byte[] topic = new byte[(int) topicLen];
            byteBuffer.get(topic);
            message.setTopicLen(topicLen);
            message.setTopic(new String(topic, Charset.defaultCharset()));

            // 17 properties
            short propertiesLength = byteBuffer.getShort();
            byte[] properties = new byte[propertiesLength];
            byteBuffer.get(properties);
            message.setPropertiesLength(propertiesLength);
            message.setProperties(new String(properties, Charset.defaultCharset()));

            ByteBuffer byteBufferMsgId = ByteBuffer.allocate(16);
            String msgId = MessageDecoder.createMessageId(byteBufferMsgId, storeHostBuf, physicOffset);
            message.setMsgId(msgId);
            return message;
        } catch (Exception e) {
            byteBuffer.position(byteBuffer.limit());
        }

        return null;
    }

}
