package com.xl.hbase;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.xl.hbase.InterpreterImpl.DataType;


/**
 * some utility methods
 */
public class Util {
	
	private static Logger LOG = Logger.getLogger(Util.class);
	/**
	 * convert a byte array to string in term of type definition
	 * @param bytes
	 * @param type
	 *            e.g., "int,int"
	 * @return
	 */
	public static String bytes2String(byte[] bytes, String type) {
		String[] types = type.split("\\,");
		StringBuilder bd = new StringBuilder();
		for (int j = 0, start = 0; j < types.length; j++) {
			String tuc = types[j].toUpperCase();
			if (tuc.equals(DataType.INT.toString())) {
				
				bd.append(Bytes.toInt(Arrays.copyOfRange(bytes, start,
						start + 4)));
				start += 4;
			} else if (tuc.equals(DataType.LONG.toString())) {
				bd.append(Bytes.toLong(Arrays.copyOfRange(bytes, start,
						start + 8)));
				start += 8;
			} else if (tuc.equals(DataType.BYTE.toString())) {
				bd.append(bytes[start]);
				start += 1;
			} else if (tuc.equals(DataType.STRING.toString())) {
				int len = Bytes.toInt(Arrays.copyOfRange(bytes, start,
						start + 4));
				bd.append(Bytes.toString(Arrays.copyOfRange(bytes, start + 4,
						start + len + 4)));
				start += len + 4;
			}
			bd.append(",");
		}
		return bd.toString();
	}

	/**
	 * convert multiple byte arrays to a string with a separator "#"
	 * @param allbytes
	 * @param type
	 * @return
	 */
	public static String bytes2String(List<byte[]> allbytes, String type) {
		assert allbytes != null && type != null;
		StringBuilder builder = new StringBuilder();

		for (int i = 0; i < allbytes.size(); ++i) {
			builder.append(bytes2String(allbytes.get(i), type));
			builder.append("#");
		}
		return builder.toString();
	}
	
	/**
	 * convert a list of string to byte array in term of type definition
	 * @param val		"12345","3234"
	 * @param type		"int,long"
	 * @return
	 */
	public static byte[] string2byte(List<String> val, String type) {
		assert val != null && type != null;

		type = type.replaceAll("\\s", "").toLowerCase();
		String[] types = type.split("\\,");
		int length = 0;
		if (types.length == val.size()) {
			ByteBuffer buff = ByteBuffer.allocate(200);
			for (int j = 0; j < types.length; ++j) {
				String tuc = types[j].toUpperCase();
				if (tuc.equals(DataType.INT.toString())) {
					int v = Integer.parseInt(val.get(j));
					buff.putInt(v);
					length += 4;
				} else if (tuc.equals(DataType.LONG.toString())) {
					long v = Long.parseLong(val.get(j));
					buff.putLong(v);
					length += 8;
				} else if (tuc.equals(DataType.BYTE.toString())) {
					byte v = Byte.parseByte(val.get(j));
					buff.put(v);
					length += 1;
				} else if (tuc.equals(DataType.STRING.toString())) {
					buff.putInt(val.get(j).length());
					byte[] v = Bytes.toBytes(val.get(j));
					buff.put(v);
					length += v.length + 4;
				}
			}
			byte[] bs = new byte[length];
			System.arraycopy(buff.array(), 0, bs, 0, length);
			return bs;
		} else
			return null;
	}


	/**
	 * convert hex string to human readable string
	 * @param hex
	 * @param typeDesc
	 */
	public static String hex2string(String hex, String typeDesc) {

		String[] types = typeDesc.split("\\,");
		char[] hex2 = getHex(hex, getLen(types));

		int start = 0;
		StringBuilder builder = new StringBuilder();
		for (int i = 0; i < types.length; ++i) {
			if (types[i].trim().equals("int")) {
				builder.append(Integer.parseInt(
						new String(Arrays.copyOfRange(hex2, start, start + 8)),
						16));
				builder.append(",");
				start += 8;
			} else if (types[i].trim().equals("long")) {
				builder.append(Long.parseLong(
								new String(Arrays.copyOfRange(hex2, start,
										start + 16)), 16));
				builder.append(",");
				start += 16;
			} else if (types[i].trim().equals("byte")) {
				builder.append(Integer.parseInt(
						new String(Arrays.copyOfRange(hex2, start, start + 2)),
						16));
				builder.append(",");
				start += 2;
			}
		}
		return builder.toString();
	}
	
	
	private static int getLen(String[] types) {
		int l = 0;
		for (int i = 0; i < types.length; ++i) {
			if (types[i].trim().equals("int")) {
				l += 8;
			} else if (types[i].trim().equals("long")) {
				l += 16;
			} else if (types[i].trim().equals("byte")) {
				l += 2;
			}
		}
		return l;
	}

	private static char[] getHex(String hex, int len) {
		char[] hexchar = new char[len];
		int i = 0;
		int j = 0;
		while (i < hex.length()) {
			int end = (i + 2 >= hex.length()) ? hex.length() : i + 2;
			if (hex.substring(i, end).equals("\\x")) {
				i = i + 2;
				hexchar[j++] = hex.charAt(i++);
				hexchar[j++] = hex.charAt(i++);
			} else {
				String sl = Integer.toHexString(hex.charAt(i++));
				if (sl.length() == 2) {
					hexchar[j++] = sl.charAt(0);
					hexchar[j++] = sl.charAt(1);
				} else
					System.err.println("invalid hex string: " + sl);
			}
		}
		return hexchar;
	}

	/**
	 * if any exception throws, return a default value. 
	 * @param value
	 * @param defaultVal
	 * @return
	 */
	public static int parseInt(String value, int defaultVal) {
		try {
			return Integer.parseInt(value);
		} catch (Exception ex) {
			LOG.error(ex.getMessage(), ex);
			return defaultVal; // set a default;
		}
	}

	

	/**
	 * if any exception throws, return a default value. 
	 * @param value
	 * @param defaultVal
	 * @return
	 */
	public static boolean parseBoolean(String value, boolean defaultVal) {
		try {
			return Boolean.parseBoolean(value);
		} catch (Exception ex) {
			LOG.error(ex.getMessage(), ex);
			return defaultVal; 
		}
	}
}