package com.coder.rocketmqclient.wrapper;

import java.io.Serializable;
import java.util.List;
import java.util.UUID;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.LocalTransactionState;
import org.apache.rocketmq.client.producer.MessageQueueSelector;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.client.producer.TransactionSendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;

import com.alibaba.fastjson.JSON;

/**
 * 消息发送器。
 * 
 * @author zhanghui
 *
 */
public final class MessageProducer implements InitializingBean, DisposableBean {

	private final static Logger logger = LoggerFactory.getLogger(MessageProducer.class);

	private static final TransactionMQProducer producer = new TransactionMQProducer();

	private String namesrvAddr = null;

	private String producerGroup;

	@Override
	public void afterPropertiesSet() throws Exception {
		checkBeforeStart();

		producer.setNamesrvAddr(namesrvAddr);
		producer.setProducerGroup(producerGroup);
		producer.setTransactionCheckListener(defaultTranCheckListener);

		try {
			producer.start();
		} catch (MQClientException e) {
			throw new RuntimeException("start client failure!:", e);
		}
	}

	private void checkBeforeStart() {
		if (null == namesrvAddr || namesrvAddr.trim().length() < 1) {
			throw new RuntimeException(
					"namesrvAddr cannot be null, please check configuration 'rocketmq.nameserver.addr'.");
		}
		if (null == producerGroup || producerGroup.trim().length() < 1) {
			throw new RuntimeException(
					"producerGroup cannot be null, please check configuration 'rocketmq.producer.group'.");
		}
	}

	/**
	 * 发送消息。
	 * 
	 * @param topic
	 *            消息topic，如order。必填。消息消费方根据topic进行订阅。
	 * @param tags
	 *            消息tags, 如createOrder。必填。消息消费方根据tags进行订阅。
	 * @param key
	 *            消息key，方便定位问题，尽量保证key的唯一，比如使用订单号，商品id等 必填。
	 * @param msg
	 *            消息内容字节数组。 消息发送者和消息接受者需要协商消息的格式以及序列化方式等。
	 * @return SendMessageResult 消息发送结果
	 * @throws IllegalArgumentException
	 *             如果必填参数为空，抛出运行时异常
	 */
	private SendMessageResult sendMessage(String topic, String tag, String key, byte[] msg) {
		checkMessage(topic, tag, key);

		final String sendId = genUniqueMsgId4Log();
		logger.info("[MessageProducer][{}]Sending Message. topic:{}, tags:{}, key:{}", sendId, topic, tag, key);

		SendResult result = null;
		try {
			result = producer.send(new Message(topic, tag, key, msg));
		} catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
			logger.warn("[MessageProducer][{}]send msg exception! topic:{}, tags:{}, key:{}, cause:{}", sendId, topic,
					tag, key, RemotingHelper.exceptionSimpleDesc(e));
			return new SendMessageResult(SendMessageResult.SEND_STATUS_FAILURE, null, null);
		}

		SendStatus status = result.getSendStatus();
		if (status == SendStatus.SEND_OK) {
			logger.info("[MessageProducer][{}] send message success! topic:{}, tags:{}, key:{}", sendId, topic, tag,
					key);
			return new SendMessageResult(SendMessageResult.SEND_STATUS_OK, result.getMsgId(), null);
		} else if (status == SendStatus.FLUSH_DISK_TIMEOUT) {
			logger.warn("[MessageProducer][{}] send message failure: flush disk timeout! topic:{}, tags:{}, key:{}",
					sendId, topic, tag, key);
			return new SendMessageResult(SendMessageResult.SEND_STATUS_FLUSH_DISK_TIMEOUT, result.getMsgId(), null);
		} else if (status == SendStatus.FLUSH_SLAVE_TIMEOUT) {
			logger.warn("[MessageProducer][{}] send message failure: flush slave timeout! topic:{}, tags:{}, key:{}",
					sendId, topic, tag, key);
			return new SendMessageResult(SendMessageResult.SEND_STATUS_FLUSH_SLAVE_TIMEOUT, result.getMsgId(), null);
		} else if (status == SendStatus.SLAVE_NOT_AVAILABLE) {
			logger.warn("[MessageProducer][{}] send message failure: slave not available! topic:{}, tags:{}, key:{}",
					sendId, topic, tag, key);
			return new SendMessageResult(SendMessageResult.SEND_STATUS_SLAVE_NOT_AVAILABLE, result.getMsgId(), null);
		}

		logger.warn("[MessageProducer][{}] send message failure: unknown reason! topic:{}, tags:{}, key:{}", sendId,
				topic, tag, key);
		return new SendMessageResult(SendMessageResult.SEND_STATUS_FAILURE, result.getMsgId(), null);
	}

	/**
	 * 发送消息。
	 * 
	 * @param topic
	 *            消息topic，如order。必填。消息消费方根据topic进行订阅。
	 * @param tag
	 *            消息tags, 如createOrder。必填。消息消费方根据tags进行订阅。
	 * @param key
	 *            消息key，方便定位问题，尽量保证key的唯一，比如使用订单号，商品id等 必填。
	 * @param msg
	 *            消息对象，可以是任何java对象。
	 * @return SendMessageResult 消息发送结果
	 * @throws IllegalArgumentException
	 *             如果必填参数为空，抛出运行时异常
	 */
	public SendMessageResult sendMessage(String topic, String tag, String key, Object msgObj) {
		logger.info("[MessageProducer]Sending message. body:{}", JSON.toJSONString(msgObj));
		byte[] body = SerializeUtil.encode(msgObj);
		return sendMessage(topic, tag, key, body);
	}

	/**
	 * 发送事务消息。
	 * 
	 * @param topic
	 *            消息topic，如order。必填。消息消费方根据topic进行订阅。
	 * @param tag
	 *            消息tags, 如createOrder。必填。消息消费方根据tags进行订阅。
	 * @param key
	 *            消息key，方便定位问题，尽量保证key的唯一，比如使用订单号，商品id等 必填。
	 * @param body
	 *            消息内容字节数组。 消息发送者和消息接受者需要协商消息的格式以及序列化方式等。
	 * @param arg
	 *            执行业务代码需要的参数
	 * @param transactionExecutor
	 *            执行业务代码的模板，业务代码写到这里。
	 * @return SendMessageResult 消息发送结果
	 * @throws IllegalArgumentException
	 *             如果必填参数为空，抛出运行时异常
	 */
	private SendMessageResult sendTransactionMessage(final String topic, final String tag, final String key,
			final byte[] body, Object arg, final LocalBusinessExecutor transactionExecutor) {
		checkMessage(topic, tag, key);
		if (transactionExecutor == null) {
			throw new IllegalArgumentException("transactionExecutor cannot be null");
		}

		final String sendId = genUniqueMsgId4Log();
		logger.info("[MessageProducer][{}]Sending transaction message. topic:{}, tags:{}, keys:{}", sendId, topic, tag,
				key);

		TransactionSendResult result = null;
		try {
			result = producer.sendMessageInTransaction(new Message(topic, tag, key, body),
					new org.apache.rocketmq.client.producer.LocalTransactionExecuter() {
						public LocalTransactionState executeLocalTransactionBranch(Message msg, Object arg) {
							LocalBusinessExecutionState localResult = null;
							try {
								// 执行业务方法
								localResult = transactionExecutor.executeLocalBusiness(arg);
							} catch (Throwable e) {
								logger.warn(
										"[MessageProducer][{}]local business execute failure, msg will be droped! topic:{}, tags:{}, keys:{}, cause:{}",
										sendId, topic, tag, key, RemotingHelper.exceptionSimpleDesc(e));
								localResult = LocalBusinessExecutionState.EXE_FAILURE;
							}
							if (localResult == null || localResult == LocalBusinessExecutionState.EXE_FAILURE) {
								return LocalTransactionState.ROLLBACK_MESSAGE; // 业务失败，回滚事务
							}
							return LocalTransactionState.COMMIT_MESSAGE;
						}
					}, arg);
		} catch (MQClientException e) {
			logger.warn("[MessageProducer][{}]send transaction msg failure! topic:{}, tags:{}, keys:{}, cause:{}",
					sendId, topic, tag, key, RemotingHelper.exceptionSimpleDesc(e));
			return new SendMessageResult(SendMessageResult.SEND_STATUS_FAILURE, null, null);
		}

		SendStatus status = result.getSendStatus();
		if (status == SendStatus.SEND_OK) {
			if (result.getLocalTransactionState() == LocalTransactionState.UNKNOW
					|| result.getLocalTransactionState() == LocalTransactionState.ROLLBACK_MESSAGE) {
				logger.warn(
						"[MessageProducer][{}]send transaction msg failure: business failure! topic:{}, tags:{}, keys:{}",
						sendId, topic, tag, key);
				return new SendMessageResult(SendMessageResult.SEND_STATUS_FAILURE, result.getMsgId(),
						result.getTransactionId());
			}
			logger.info("[MessageProducer][{}]send transaction msg success! topic:{}, tags:{}, keys:{}", sendId, topic,
					tag, key);
			return new SendMessageResult(SendMessageResult.SEND_STATUS_OK, result.getMsgId(),
					result.getTransactionId());
		} else if (status == SendStatus.FLUSH_DISK_TIMEOUT) {
			logger.warn(
					"[MessageProducer][{}]send transaction message failure: flush disk timeout! topic:{}, tags:{}, keys:{}",
					sendId, topic, tag, key);
			return new SendMessageResult(SendMessageResult.SEND_STATUS_FLUSH_DISK_TIMEOUT, result.getMsgId(),
					result.getTransactionId());
		} else if (status == SendStatus.FLUSH_SLAVE_TIMEOUT) {
			logger.warn(
					"[MessageProducer][{}]send transaction message failure: flush slave timeout! topic:{}, tags:{}, keys:{}",
					sendId, topic, tag, key);
			return new SendMessageResult(SendMessageResult.SEND_STATUS_FLUSH_SLAVE_TIMEOUT, result.getMsgId(),
					result.getTransactionId());
		} else if (status == SendStatus.SLAVE_NOT_AVAILABLE) {
			logger.warn(
					"[MessageProducer][{}]send transaction message failure: slave not timeout! topic:{}, tags:{}, keys:{}",
					sendId, topic, tag, key);
			return new SendMessageResult(SendMessageResult.SEND_STATUS_SLAVE_NOT_AVAILABLE, result.getMsgId(),
					result.getTransactionId());
		}

		logger.warn("[MessageProducer][{}]send transaction message failure: unknown reason! topic:{}, tags:{}, keys:{}",
				sendId, topic, tag, key);
		return new SendMessageResult(SendMessageResult.SEND_STATUS_FAILURE, result.getMsgId(),
				result.getTransactionId());
	}

	/**
	 * 发送事务消息。
	 * 
	 * @param topic
	 *            消息topic，如order。必填。消息消费方根据topic进行订阅。
	 * @param tag
	 *            消息tags, 如createOrder。必填。消息消费方根据tags进行订阅。
	 * @param key
	 *            消息key，方便定位问题，尽量保证key的唯一，比如使用订单号，商品id等 必填。
	 * @param msgObj
	 *            消息对象，可以是任何java对象。序列化方式参考方法说明。
	 * @param arg
	 *            执行业务代码需要的参数
	 * @param transactionExecutor
	 *            执行业务代码的模板，业务代码写到这里。
	 * @return SendMessageResult 消息发送结果
	 * @throws IllegalArgumentException
	 *             如果必填参数为空，抛出运行时异常
	 */
	public SendMessageResult sendTransactionMessage(final String topic, final String tag, final String key,
			final Object msgObj, Object arg, final LocalBusinessExecutor transactionExecutor) {
		logger.info("[MessageProducer]Sending transaction message. body:{}, arg{}", JSON.toJSONString(msgObj),
				JSON.toJSONString(arg));
		byte[] body = SerializeUtil.encode(msgObj);
		return sendTransactionMessage(topic, tag, key, body, arg, transactionExecutor);
	}

	// ********************************
	// 顺序消息
	// ********************************
	/**
	 * 队列选择器。用户如果想发送顺序消息，必须实现该接口。
	 * 
	 * @author zhanghui
	 *
	 */
	public static interface QueueSelector {
		/**
		 * 自己提供算法，从queueSize个队列中选择一个队列用来发送消息。
		 * 
		 * @param queueSize
		 *            当前topic的消息在MQ服务器端的队列的个数。
		 * @param topic
		 *            调用sendOrderMessage传的topic
		 * @param tag
		 *            调用sendOrderMessage传的tag
		 * @param key
		 *            调用sendOrderMessage传的key
		 * @param msgObj
		 *            调用sendOrderMessage传的msgObj
		 * @return 被选中的队列在queueSize个队列中的下标。取值范围[0,queueSize-1]。
		 *         如果小于0，默认取0。如果大于queueSize-1，默认取queueSize-1
		 */
		int select(int queueSize, String topic, String tag, String key, Object msgObj);
	}

	/**
	 * 发送消息。
	 * 
	 * @param topic
	 *            消息topic，如order。必填。消息消费方根据topic进行订阅。
	 * @param tag
	 *            消息tags, 如createOrder。必填。消息消费方根据tags进行订阅。
	 * @param key
	 *            消息key，方便定位问题，尽量保证key的唯一，比如使用订单号，商品id等 必填。
	 * @param msgObj
	 *            消息对象，可以是任何java对象。
	 * @param selector
	 *            自定义队列选择器。
	 * @return SendMessageResult 消息发送结果
	 * @throws IllegalArgumentException
	 *             如果必填参数为空，抛出运行时异常
	 */
	public SendMessageResult sendOrderMessage(String topic, String tag, String key, Object msgObj,
			QueueSelector selector) {
		if (selector == null) {
			throw new RuntimeException("param 'selector' cannot be null");
		}
		checkMessage(topic, tag, key);

		final String sendId = genUniqueMsgId4Log();
		logger.info("[MessageProducer][{}]Sending order Message. topic:{}, tags:{}, key:{}", sendId, topic, tag, key);

		SendResult result = null;
		try {
			byte[] body = SerializeUtil.encode(msgObj);
			result = producer.send(new Message(topic, tag, key, body), new MessageQueueSelector() {
				public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
					int index = selector.select(mqs.size(), msg.getTopic(), msg.getTags(), msg.getKeys(), arg);
					if (index < 0) {
						index = 0;
					} else if (index > mqs.size() - 1) {
						index = mqs.size() - 1;
					}
					return mqs.get(index);
				}
			}, msgObj);
		} catch (MQClientException | RemotingException | MQBrokerException | InterruptedException e) {
			logger.warn("[MessageProducer][{}]send msg exception! topic:{}, tags:{}, key:{}, cause:{}", sendId, topic,
					tag, key, RemotingHelper.exceptionSimpleDesc(e));
			return new SendMessageResult(SendMessageResult.SEND_STATUS_FAILURE, null, null);
		}

		SendStatus status = result.getSendStatus();
		if (status == SendStatus.SEND_OK) {
			logger.info("[MessageProducer][{}] send message success! topic:{}, tags:{}, key:{}", sendId, topic, tag,
					key);
			return new SendMessageResult(SendMessageResult.SEND_STATUS_OK, result.getMsgId(), null);
		} else if (status == SendStatus.FLUSH_DISK_TIMEOUT) {
			logger.warn("[MessageProducer][{}] send message failure: flush disk timeout! topic:{}, tags:{}, key:{}",
					sendId, topic, tag, key);
			return new SendMessageResult(SendMessageResult.SEND_STATUS_FLUSH_DISK_TIMEOUT, result.getMsgId(), null);
		} else if (status == SendStatus.FLUSH_SLAVE_TIMEOUT) {
			logger.warn("[MessageProducer][{}] send message failure: flush slave timeout! topic:{}, tags:{}, key:{}",
					sendId, topic, tag, key);
			return new SendMessageResult(SendMessageResult.SEND_STATUS_FLUSH_SLAVE_TIMEOUT, result.getMsgId(), null);
		} else if (status == SendStatus.SLAVE_NOT_AVAILABLE) {
			logger.warn("[MessageProducer][{}] send message failure: slave not available! topic:{}, tags:{}, key:{}",
					sendId, topic, tag, key);
			return new SendMessageResult(SendMessageResult.SEND_STATUS_SLAVE_NOT_AVAILABLE, result.getMsgId(), null);
		}

		logger.warn("[MessageProducer][{}] send message failure: unknown reason! topic:{}, tags:{}, key:{}", sendId,
				topic, tag, key);
		return new SendMessageResult(SendMessageResult.SEND_STATUS_FAILURE, result.getMsgId(), null);

	}

	private void checkMessage(String topic, String tag, String key) {
		if (null == topic || topic.trim().length() < 1) {
			throw new IllegalArgumentException("topic cannot be null");
		}
		if (null == tag || tag.trim().length() < 1) {
			throw new IllegalArgumentException("tag cannot be null");
		}
		if (null == key || key.trim().length() < 1) {
			throw new IllegalArgumentException("key cannot be null");
		}
	}

	/**
	 * 每次发消息都会使用唯一的字符串来打日志，方便定位问题
	 * 
	 * @return
	 */
	private String genUniqueMsgId4Log() {
		return UUID.randomUUID().toString();
	}

	@Override
	public void destroy() throws Exception {
		if (producer != null) {
			producer.shutdown();
		}
	}

	public String getNamesrvAddr() {
		return namesrvAddr;
	}

	public void setNamesrvAddr(String namesrvAddr) {
		this.namesrvAddr = namesrvAddr;
	}

	public String getProducerGroup() {
		return producerGroup;
	}

	public void setProducerGroup(String producerGroup) {
		this.producerGroup = producerGroup;
	}

	/**
	 * 本地业务执行模板。
	 */
	public static interface LocalBusinessExecutor {
		/**
		 * 执行本地业务方法。<br/>
		 * 建议： 1.业务方法尽量简短，保证本地事务快速执行完 2.不建议抛异常，尽量自行try-catch
		 * 
		 * @param arg
		 *            执行本地业务需要的参数
		 * @return 业务成功返回LocalBusinessState.EXE_OK。
		 *         返回null或者或者抛出异常或者返回EXE_FAILURE都表示业务失败。
		 */
		LocalBusinessExecutionState executeLocalBusiness(Object arg);
	}

	public static enum LocalBusinessExecutionState {
		EXE_OK, // 业务执行成功
		EXE_FAILURE; // 业务执行失败
	}

	/**
	 * 消息服务器回调接口。其实回调的情况很少发生(比如业务执行失败的时候突然宕机，导致事务状态没有发给消息服务器)，一旦发生，这里假设业务已经成功了，
	 * 让消息继续发送。 如果有什么问题，可以走线下恢复。
	 */
	public static final TransactionCheckListener defaultTranCheckListener = new TransactionCheckListener() {

		@Override
		public LocalTransactionState checkLocalTransactionState(MessageExt msg) {
			return LocalTransactionState.COMMIT_MESSAGE;
		}

	};

	public static class SendMessageResult implements Serializable {
		public static final int SEND_STATUS_OK = 1;
		public static final int SEND_STATUS_FLUSH_DISK_TIMEOUT = 2;
		public static final int SEND_STATUS_FLUSH_SLAVE_TIMEOUT = 3;
		public static final int SEND_STATUS_SLAVE_NOT_AVAILABLE = 4;
		public static final int SEND_STATUS_FAILURE = 0;

		private static final long serialVersionUID = -4904199170361552907L;

		private int status; // 消息发送状态： 1-消息发送成功 0-失败 2-消息服务器刷盘超时 3-消息服务器备份超时
							// 4-消息slave服务器不可用
		private String msgId; // 消息id。对于每条消息，消息服务器都会生成一个唯一的消息id。
		private String transactionId; // 事务id(如果发送的是事务消息，该字段不为空)

		public SendMessageResult() {
		}

		public SendMessageResult(int status, String msgId, String transactionId) {
			this.status = status;
			this.msgId = msgId;
			this.transactionId = transactionId;
		}

		public int getStatus() {
			return status;
		}

		public void setStatus(int status) {
			this.status = status;
		}

		public String getMsgId() {
			return msgId;
		}

		public void setMsgId(String msgId) {
			this.msgId = msgId;
		}

		public String getTransactionId() {
			return transactionId;
		}

		public void setTransactionId(String transactionId) {
			this.transactionId = transactionId;
		}

		@Override
		public String toString() {
			return "SendMessageResult [status=" + status + ", msgId=" + msgId + ", transactionId=" + transactionId
					+ "]";
		}
	}

}
