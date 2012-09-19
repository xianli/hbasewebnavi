package com.xl.hbase;

import junit.framework.Assert;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

public class TestInterpreter {

	Interpreter intp = new InterpreterImpl();
	
	@Test public void testInterpreterRK() {
		String sv = intp.interpretRowkey("TEST", Bytes.add(Bytes.toBytes(23456), 
				Bytes.toBytes(12345)));
		Assert.assertEquals("23456,12345,", sv);
		
		String sv2 = intp.interpretRowkey("TEST2", Bytes.add(Bytes.toBytes(23456), 
				Bytes.toBytes(12345.2d)));
		Assert.assertEquals("23456,12345.2,", sv2);
	}
	
	@Test public void testInterpreterCV() {
		byte[] value = Bytes.add(Bytes.toBytes(2), 
				Bytes.add(Bytes.toBytes(23456), 
				Bytes.toBytes(12345)));
		String sv = intp.interpretColumnValue("TEST", null, Bytes.toBytes("d"), 
				Bytes.toBytes("PV"), 0l, value);
		Assert.assertEquals("[23456,12345]", sv);
		
		byte[] value2 = Bytes.add(Bytes.toBytes(2), 
				Bytes.add(Bytes.toBytes(23456.1d), 
				Bytes.toBytes(12345.2d)));
		String sv2 = intp.interpretColumnValue("TEST", null, Bytes.toBytes("d"), 
				Bytes.toBytes("Money"), 0l, value2);
		Assert.assertEquals("[23456.1,12345.2]", sv2);
	}
	
}
