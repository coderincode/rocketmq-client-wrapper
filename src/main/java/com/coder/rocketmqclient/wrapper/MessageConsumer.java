package com.coder.rocketmqclient.wrapper;

import java.lang.reflect.ParameterizedType;
import java.util.List;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.consumer.listener.MessageListenerOrderly;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.hook.ConsumeMessageContext;
import org.apache.rocketmq.client.hook.ConsumeMessageHook;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.alibaba.fastjson.JSON;

/**
 * 消息消费者。
 * 
 * @author zhanghui
 *
 */
public class MessageConsumer implements InitializingBean, DisposableBean {

	private final static Logger logger = LoggerFactory.getLogger(MessageConsumer.class);

	public static final String MESSAGE_MODEL_CLUSTER = "cluster";
	
	public static final String MESSAGE_MODEL_BROADCAST = "broadcast";
	
	private final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer();

	private String namesrvAddr;
	
	private String consumerGroup;

	private String topic;
	
	private String tag = "*";

	private int consumeMessageBatchMaxSize = 1;
	
	/**
	 * 是否顺序消息 0-不是 1-是
	 */
	private int orderedFlag = 0;
	
	/**
	 * 消费模式：cluster-集群模式   broadcast-广播模式
	 */
	private String messageModel = MESSAGE_MODEL_CLUSTER;
	
	@SuppressWarnings("rawtypes")
	private ConsumeMessageListener listener;

	// 消息消费拦截器
	private ConsumeMessageHook consumeMessageHook = defaultConsumeMessageHook;
	
	private static final ConsumeMessageHook defaultConsumeMessageHook = new ConsumeMessageHook(){

		@Override
		public String hookName() {
			return "defaultConsumeMessageHook";
		}

		@Override
		public void consumeMessageBefore(ConsumeMessageContext context) {
			logger.debug("[MessageConsumer]before consume: " + JSON.toJSONString(context));
		}

		@Override
		public void consumeMessageAfter(ConsumeMessageContext context) {
			logger.debug("[MessageConsumer]after consume: " + JSON.toJSONString(context));
		}
	};
	
	
	@Override
	public void afterPropertiesSet() throws Exception {
		checkBeforeStart();
		
		consumer.setNamesrvAddr(namesrvAddr);
		consumer.setConsumerGroup(consumerGroup);
		
		consumer.getDefaultMQPushConsumerImpl().registerConsumeMessageHook(this.consumeMessageHook);

		if (messageModel.trim().equalsIgnoreCase(MESSAGE_MODEL_CLUSTER)) {
			consumer.setMessageModel(MessageModel.CLUSTERING);
		}
		if (messageModel.trim().equalsIgnoreCase(MESSAGE_MODEL_BROADCAST)) {
			consumer.setMessageModel(MessageModel.BROADCASTING);
		}
		
		consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
		consumer.setConsumeMessageBatchMaxSize(consumeMessageBatchMaxSize);

		try {
			consumer.subscribe(topic, tag);
		} catch (MQClientException e) {
			logger.error("[MessageConsumer]subscribe failure! topic:{}, tag:{}, cause:{}", 
					topic, tag, RemotingHelper.exceptionSimpleDesc(e));
			throw new RuntimeException(e.getMessage(), e);
		}
		
		if (this.orderedFlag == 0) { // 非顺序消息
			registerNonOrderedListener();
		} else { // 顺序消息
			registerOrderedListener();
		}

		try {
			consumer.start();
		} catch (MQClientException e) {
			throw new RuntimeException("Init rocketmq consumer client failure! cause: " + e.getMessage(), e);
		}
	}
	
	
	
	private void checkBeforeStart() {
		if (null == namesrvAddr || namesrvAddr.trim().length() < 1) {
			throw new IllegalArgumentException(
					"namesrvAddr cannot be null, please check configuration 'rocketmq.nameserver.addr'.");
		}
		if (null == consumerGroup || consumerGroup.trim().length() < 1) {
			throw new IllegalArgumentException(
					"consumerGroup cannot be null, please check configuration 'rocketmq.consumer.group'.");
		}
		if (null == topic || topic.trim().length() < 1) {
			throw new IllegalArgumentException("topic cannot be null");
		}
		if (messageModel == null || messageModel.trim().length() < 1) {
			throw new IllegalArgumentException("messageModel cannot be null");
		}
		if (!messageModel.trim().equalsIgnoreCase(MESSAGE_MODEL_CLUSTER) && !messageModel.trim().equalsIgnoreCase(MESSAGE_MODEL_BROADCAST)) {
			throw new IllegalArgumentException("invalid value of messageModel, expect 'cluster' or 'broadcast'.");
		}
		if (listener == null) {
			throw new IllegalArgumentException("consumer listener cannot be null");
		}
	}

	/**
	 * 非顺序消息
	 */
	private void registerNonOrderedListener() {
		consumer.registerMessageListener(new MessageListenerConcurrently() {
			@SuppressWarnings("unchecked")
			public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext Context) {
				// 泛型实际类型
				Class<?> genericClz = (Class<?>) ((ParameterizedType) listener.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0];
				for (MessageExt msg : list) {
					Object obj = SerializeUtil.decode(msg.getBody(), genericClz);
					try {
						logger.info("[MessageConsumer]Consuming message... class:[{}], body:[{}], msg:[{}]", genericClz, JSON.toJSONString(obj), msg); 
						ConsumeMessageResult consumeResult = listener.onMessageReceive(
								msg.getTopic(),
								msg.getTags(),
								msg.getKeys(),
								msg.getMsgId(),
								msg.getBornTimestamp(),
								obj);
						
						if (consumeResult == null || consumeResult == ConsumeMessageResult.CONSUME_FAILURE) {
							// 只要有一条消息消费报错，就会返回消费失败，所有消息消费成功才算成功
							logger.error("[MessageConsumer]Consuming failure! class:[{}], body:[{}], msg:{}", genericClz, JSON.toJSONString(obj), msg); 
							return ConsumeConcurrentlyStatus.RECONSUME_LATER;
						}
					} catch (Throwable e) {
						logger.error("[MessageConsumer]Consuming failure! "
								+ "  cause: " + e.getMessage() 
								+ "; class:" + genericClz 
								+ "; body:" + JSON.toJSONString(obj) 
								+ ", msg: " + msg, e);
						return ConsumeConcurrentlyStatus.RECONSUME_LATER;
					}
					logger.info("[MessageConsumer]Consuming message success! class:[{}], body:[{}], msg:{}", genericClz, JSON.toJSONString(obj), msg); 
				}
				
				return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
			}
		});
	}
	
	/**
	 * 顺序消费
	 */
	private void registerOrderedListener() {
		consumer.registerMessageListener(new MessageListenerOrderly(){
			@SuppressWarnings("unchecked")
			public ConsumeOrderlyStatus consumeMessage(List<MessageExt> list, ConsumeOrderlyContext context) {
				// 泛型实际类型
				Class<?> genericClz = (Class<?>) ((ParameterizedType) listener.getClass().getGenericInterfaces()[0]).getActualTypeArguments()[0];
				for (MessageExt msg : list) {
					Object obj = SerializeUtil.decode(msg.getBody(), genericClz);
					try {
						logger.info("[MessageConsumer]Consuming message... class:[{}], body:[{}], msg:[{}]", genericClz, JSON.toJSONString(obj), msg); 
						ConsumeMessageResult consumeResult = listener.onMessageReceive(
								msg.getTopic(),
								msg.getTags(),
								msg.getKeys(),
								msg.getMsgId(),
								msg.getBornTimestamp(),
								obj);
						
						if (consumeResult == null || consumeResult == ConsumeMessageResult.CONSUME_FAILURE) {
							// 只要有一条消息消费报错，就会返回消费失败，所有消息消费成功才算成功
							logger.error("[MessageConsumer]Consuming failure! class:[{}], body:[{}], msg:{}", genericClz, JSON.toJSONString(obj), msg); 
							return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
						}
					} catch (Throwable e) {
						logger.error("[MessageConsumer]Consuming failure! "
								+ "  cause: " + e.getMessage() 
								+ "; class:" + genericClz 
								+ "; body:" + JSON.toJSONString(obj) 
								+ ", msg: " + msg, e);
						return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
					}
					logger.info("[MessageConsumer]Consuming message success! class:[{}], body:[{}], msg:{}", genericClz, JSON.toJSONString(obj), msg); 
				}
				return ConsumeOrderlyStatus.SUCCESS;
			}
			
		});
	}
	
	public String getNamesrvAddr() {
		return namesrvAddr;
	}

	public void setNamesrvAddr(String namesrvAddr) {
		this.namesrvAddr = namesrvAddr;
	}

	public String getConsumerGroup() {
		return consumerGroup;
	}

	public void setConsumerGroup(String consumerGroup) {
		this.consumerGroup = consumerGroup;
	}

	public int getOrderedFlag() {
		return orderedFlag;
	}

	public void setOrderedFlag(int orderedFlag) {
		this.orderedFlag = orderedFlag;
	}

	public String getMessageModel() {
		return messageModel;
	}

	public void setMessageModel(String messageModel) {
		this.messageModel = messageModel;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public String getTag() {
		return tag;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public int getConsumeMessageBatchMaxSize() {
		return consumeMessageBatchMaxSize;
	}

	public void setConsumeMessageBatchMaxSize(int consumeMessageBatchMaxSize) {
		this.consumeMessageBatchMaxSize = consumeMessageBatchMaxSize;
	}

	@SuppressWarnings("rawtypes")
	public ConsumeMessageListener getListener() {
		return listener;
	}

	@SuppressWarnings("rawtypes")
	public void setListener(ConsumeMessageListener listener) {
		this.listener = listener;
	}

	@Override
	public void destroy() throws Exception {
		if (consumer != null) {
			consumer.shutdown();
		}
	}

	/**
	 * 消息消费listener
	 * 
	 * @author zhanghui
	 *
	 */
	public static interface ConsumeMessageListener<T> {

		/**
		 * 消息消费callback. <br/>
		 * 建议:
		 * 1. 建议不要抛出异常，尽量自己try-catch.
		 * 2. RocketMQ在某些情况下会发重复消息，建议消费端实现幂等处理。
		 * 
		 * @param topic 
		 * 			消息topic。和消息发送端一致。
		 * @param tag
		 * 			消息tag。和消息发送端一致。
		 * @param key
		 * 			消息key。和消息发送端一致。
		 * @param msgId
		 *            消息服务器生成的唯一消息id，建议消费端进行幂等处理
		 * @param bornTime
		 * 			消息在发送端生成时的时间戳
		 * @param msg
		 *            消息
		 * @return 消息消费成功返回ConsumeMessageResult.CONSUME_OK，
		 *         返回null或者抛异常或者返回CONSUME_FAILURE都表示消息消费失败。
		 */
		ConsumeMessageResult onMessageReceive(String topic, String tag, String key, String msgId, long bornTime, T msg);
	}

	public static enum ConsumeMessageResult {
		CONSUME_OK, // 消息消费成功
		CONSUME_FAILURE; // 消息消费失败
	}
}
