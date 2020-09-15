package com.geca.kafka;

import java.util.Map;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JsonSerilizer<T> implements Serializer<T>{
	private final ObjectMapper objectMapper = new ObjectMapper();
	public JsonSerilizer() {};
	
	public void close() {}

	public void configure(Map<String, ?> arg0, boolean arg1) {}

	public byte[] serialize(String topic, T data) {
		// TODO Auto-generated method stub
		if(data == null)
			return null;
		try {
			return objectMapper.writeValueAsBytes(data);
		}catch(Exception e) {
			throw new SerializationException("Error while serializing kafka message : ", e);
		}
	}

}
