package com.gsd.pig.udf;

import java.io.IOException;

import junit.framework.Assert;

import org.junit.Test;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

public class ParseBagTest {
	
	@Test
	public void Bagとtupleがパースできること () throws IOException {

		ParseBag ps = new ParseBag();
		Tuple tp1 = TupleFactory.getInstance().newTuple();
		Tuple tp2= TupleFactory.getInstance().newTuple();
		Tuple tp3 = TupleFactory.getInstance().newTuple();
		tp1.append("00");
		tp1.append("01");
		tp1.append("02");
		tp1.append("03");
		tp2.append("10");
		tp2.append("11");
		tp2.append("12");
		tp2.append("13");
		tp3.append("20");
		tp3.append("21");
		tp3.append("22");
		tp3.append("23");
		DataBag data = BagFactory.getInstance().newDefaultBag();
		data.add(tp1);
		data.add(tp2);
		data.add(tp3);
		String value = ps.get(data,"|","-");
		System.out.println(value.toString());
		Assert.assertEquals("00-01-02-03|10-11-12-13|20-21-22-23", value.toString());
	}

}
