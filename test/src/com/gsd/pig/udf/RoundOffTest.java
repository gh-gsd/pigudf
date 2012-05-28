package com.gsd.pig.udf;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;

public class RoundOffTest {
	
	@Test
	public void test_少数第5位で四捨五入できること () throws IOException {
		Double nu = 23.746588;
		RoundOff rf = new RoundOff();
		double value = rf.get(nu);
		System.out.println(value);
		Assert.assertEquals(23.74659, value);
	}

}
