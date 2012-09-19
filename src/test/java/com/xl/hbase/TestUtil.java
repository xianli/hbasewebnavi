package com.xl.hbase;

import java.util.ArrayList;
import java.util.List;

import junit.framework.Assert;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestUtil {

	@Test public void testHex2String() {
		Assert.assertEquals("11956,20120604,", 
				Util.hex2string("\\x00\\x00.\\xB4\\x013\\x04\\x1C", "int,int"));
		Assert.assertEquals("10031,20120704,", 
				Util.hex2string("\\x00\\x00'/\\x013\\x04\\x80", "int,int"));
	}
	
	
	@Test public void testBytes2String() {
		Assert.assertEquals("1234,2345,", 
				Util.bytes2String(Bytes.add(Bytes.toBytes(1234), 
						Bytes.toBytes(2345)), "int,int"));
		
		Assert.assertEquals("12345678901,102345678901,", 
				Util.bytes2String(Bytes.add(Bytes.toBytes(12345678901l), 
				Bytes.toBytes(102345678901l)), "long,long"));
	}
	
	@Test public void testString2Bytes() {
		List<String> values = new ArrayList<String>();
		values.add("12345");
		values.add("34567");
		byte[] actual = Util.string2byte(values, "int,int");
		byte[] expected = Bytes.add(Bytes.toBytes(12345), Bytes.toBytes(34567));
		Assert.assertEquals(Bytes.compareTo(expected, actual)==0, true);
	}
}
