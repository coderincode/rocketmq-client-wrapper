package example;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.UUID;

import com.alibaba.fastjson.JSON;
import com.coder.rocketmqclient.wrapper.MessageProducer;
import com.coder.rocketmqclient.wrapper.MessageProducer.SendMessageResult;

public class ProducerTest {

	public static void main(String[] args) throws Exception {
		test1();
	}

	// 发简单消息
	public static void test1() throws Exception {
		MessageProducer producer = new MessageProducer();
		producer.setNamesrvAddr("192.168.11.29:9878");
		producer.setProducerGroup("producer_group_mygroup1");
		producer.afterPropertiesSet();

		for (int i = 0; i < 10; i++) {
			SendMessageResult result = producer.sendMessage(
					"mytopic1", "mytag1", "key-msg"+i, "body-msg" + i);
			System.out.println("send mytopic-mytag message successed: " + result);

			Thread.sleep(5000);
		}
		

		producer.destroy();
	}

	// 加盟商区域绑定消息
	public static void test2() throws Exception {
		MessageProducer producer = new MessageProducer();
		producer.setNamesrvAddr("192.168.11.29:9878");
		producer.setProducerGroup("producer_group_test");
		producer.afterPropertiesSet();

		Person msgObj = new Person(1, 33, "allei", Arrays.asList("杭州", "南京"));
		SendMessageResult result3 = producer.sendMessage("objectTopic", "createObject",
				UUID.randomUUID().toString(), msgObj);
		System.out.println("sending message: " + JSON.toJSONString(msgObj));
		System.out.println("send objectTopic-createObject message successed: " + result3);

		producer.destroy();
	}

}
