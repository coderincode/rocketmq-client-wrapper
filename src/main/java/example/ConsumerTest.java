package example;

import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.fastjson.JSON;
import com.coder.rocketmqclient.wrapper.MessageConsumer;
import com.coder.rocketmqclient.wrapper.MessageConsumer.ConsumeMessageListener;
import com.coder.rocketmqclient.wrapper.MessageConsumer.ConsumeMessageResult;

/**
 * 消息消费者。
 * 
 * @author zhanghui
 *
 */
public class ConsumerTest {

	public static void main(String[] args) throws Exception {
		test1();
	}
	
	// 消费普通消息
	public static void test1() throws Exception {
		final AtomicInteger count = new AtomicInteger();

		MessageConsumer consumer = new MessageConsumer();
		consumer.setNamesrvAddr("192.168.11.29:9878");
		consumer.setConsumerGroup("consumer_group_mygroup1");
		consumer.setTopic("mytopic1");
		consumer.setTag("mytag1");

		consumer.setListener(new ConsumeMessageListener<String>() {
			@Override
			public ConsumeMessageResult onMessageReceive(String topic, String tag, String key, String msgId,
					long bornTime, String msg) {
				System.out.println(count.incrementAndGet() + "--Recieve msg[topic="+topic+"]: key["+key+"]" + JSON.toJSONString(msg));
				return ConsumeMessageResult.CONSUME_OK;
			}
		});
		consumer.afterPropertiesSet();
		System.out.println("Consumer boot success!");

		Thread.sleep(3000000);
		consumer.destroy();
	}
	
	public static void test2() throws Exception {
		final AtomicInteger count = new AtomicInteger();

		MessageConsumer consumer = new MessageConsumer();
		consumer.setNamesrvAddr("192.168.11.29:9878");
		consumer.setConsumerGroup("consumer_group_test22");
		consumer.setTopic("ALLIANCE_REGION_TOPIC");
		consumer.setTag("REGION_DEPLOY");

		consumer.setListener(new ConsumeMessageListener<Person>() {
			@Override
			public ConsumeMessageResult onMessageReceive(String topic, String tag, String key, String msgId,
					long bornTime, Person msg) {
				System.out.println(count.incrementAndGet() + "--msgId:[" + msgId + "],topic:[" + topic + "],tag:[" + tag + "].Recieve msg: " + JSON.toJSONString(msg));
				return ConsumeMessageResult.CONSUME_OK;
			}
		});
		consumer.afterPropertiesSet();
		System.out.println("Consumer2 boot success!");

		Thread.sleep(300000);
		consumer.destroy();
	}
}
