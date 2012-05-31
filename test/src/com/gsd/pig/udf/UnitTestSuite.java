package com.gsd.pig.udf;

import org.junit.runner.JUnitCore;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

/**
 * 単体テストスイート
 * 
 * @author gsd
 *
 */
@RunWith(Suite.class)

@SuiteClasses( { 
	GetReqParamTest.class,
	RoundOffTest.class,
	DateTimeConverterTest.class,
	ParseSearchTermTest.class 
})
public class UnitTestSuite {
	public static void main(String[] args) {
		JUnitCore.main(UnitTestSuite.class.getName());
	}
}
