package com.gsd.pig.udf;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

public class DateTimeConverterTest {

	@Test
	public void test_UNIXTIMEを変換できること () throws IOException {
		String unixTime = "1329876382000";
		long time = Long.parseLong(unixTime);
		DateTimeConverter dt = new DateTimeConverter();
		String value = dt.get(time);
		System.out.println(value);
		Assert.assertEquals("2012022211", value);
	}
}
