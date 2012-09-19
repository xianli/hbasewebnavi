package com.xl.hbase;

import junit.framework.Assert;

import org.apache.hadoop.hbase.util.Bytes;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;

public class TestReader {
	Reader reader ;
	@Before public void init() {
		reader = new Reader("TEST");
	}
	
	@Test public void testGet() throws JSONException {
		JSONArray ret = reader.get(Bytes.toBytes(1));
		System.out.println(ret);
		JSONObject first = (JSONObject)ret.get(0);
		Object value = first.get("1,");
		Assert.assertNotNull(value);
		Assert.assertNotNull(first.get("524288,657391923,"));
	}
	
	@Test public void testScan() {
		JSONArray ret = reader.scan(Bytes.add(Bytes.toBytes(10031), 
				Bytes.toBytes(20120531)), Bytes.add(Bytes.toBytes(10031), 
						Bytes.toBytes(20120626)));
		System.out.println(ret.toString());
	}
	
	@Test public void testCount() {
		Assert.assertEquals(24, reader.count());
	}
}
