package com.tracker.storm.data.scheme;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class StringScheme implements Scheme {
	private static final long serialVersionUID = -322088285958764197L;

	public List<Object> deserialize(byte[] bytes) {
        try {
            return new Values(new String(bytes, "UTF-8"));
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    public Fields getOutputFields() {
        return new Fields("string");
    }
}