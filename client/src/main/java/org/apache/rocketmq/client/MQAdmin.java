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
package org.apache.rocketmq.client;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.exception.RemotingException;

/**
 * Base interface for MQ management
 */
public interface MQAdmin {
    /**
     * 创建topic
     * @param key 用于消息检索，可以与newTopic相同
     * @param newTopic topic名称
     * @param queueNum 队列数量
     * @throws MQClientException
     */
    void createTopic(final String key, final String newTopic, final int queueNum)
        throws MQClientException;

    /**
     *
     * 创建topic
     * @param key 用于消息检索，可以与newTopic相同
     * @param newTopic topic名称
     * @param queueNum 队列数量
     * @param topicSysFlag 主题系统标签，默认为0，参考：
     * @throws MQClientException
     */
    void createTopic(String key, String newTopic, int queueNum, int topicSysFlag)
        throws MQClientException;

    /**
     * Gets the message queue offset according to some time in milliseconds<br>
     * be cautious to call because of more IO overhead
     * 根据时间戳从队列中查找其偏移量
     * @param mq Instance of MessageQueue
     * @param timestamp from when in milliseconds.
     * @return offset
     */
    long searchOffset(final MessageQueue mq, final long timestamp) throws MQClientException;

    /**
     * 获取该队列中最大物理偏移量
     *
     * @param mq Instance of MessageQueue
     * @return the max offset
     */
    long maxOffset(final MessageQueue mq) throws MQClientException;

    /**
     * 获取该队列中最小物理偏移量
     *
     * @param mq Instance of MessageQueue
     * @return the minimum offset
     */
    long minOffset(final MessageQueue mq) throws MQClientException;

    /**
     * Gets the earliest stored message time
     *
     * @param mq Instance of MessageQueue
     * @return the time in microseconds
     */
    long earliestMsgStoreTime(final MessageQueue mq) throws MQClientException;

    /**
     * Query message according to message id
     *
     * @param offsetMsgId message id
     * @return message
     */
    MessageExt viewMessage(final String offsetMsgId) throws RemotingException, MQBrokerException,
        InterruptedException, MQClientException;

    /**
     * 查询消息
     *
     * @param topic message topic
     * @param key message key index word
     * @param maxNum max message number 本次最多去除多少条消息
     * @param begin from when 开始时间
     * @param end to when 结束时间
     * @return Instance of QueryResult
     */
    QueryResult queryMessage(final String topic, final String key, final int maxNum, final long begin,
        final long end) throws MQClientException, InterruptedException;

    /**
     * @return The {@code MessageExt} of given msgId
     */
    MessageExt viewMessage(String topic,
        String msgId) throws RemotingException, MQBrokerException, InterruptedException, MQClientException;

}