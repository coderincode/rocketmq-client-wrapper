package com.coder.rocketmqclient.wrapper;

import java.nio.charset.Charset;

import com.alibaba.fastjson.JSON;

/**
 * 
 * @author zhanghui
 *
 */
public final class SerializeUtil {
	public final static Charset CHARSET_UTF8 = Charset.forName("UTF-8");
	
	private SerializeUtil() {

	}

	public static byte[] encode(final Object obj) {
		return toJson(obj).getBytes(CHARSET_UTF8);
	}
	
	public static String toJson(final Object obj) {
		return JSON.toJSONString(obj, false);
	}

	public static <T> T decode(final byte[] data, Class<T> classOfT) {
		final String json = new String(data, CHARSET_UTF8);
		return fromJson(json, classOfT);
	}

	public static <T> T fromJson(String json, Class<T> classOfT) {
		return JSON.parseObject(json, classOfT);
	}

}
