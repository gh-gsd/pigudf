package com.gsd.pig.udf;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.pig.data.DataBag;
import org.junit.Test;

public class ParseSearchTermTest {

	@Test
	public void Bagで2つ返ってくること () throws IOException {

		ParseSearchTerm ps = new ParseSearchTerm();
		DataBag value = ps.get("あいうえお かきくけこ");
		System.out.println(value.toString());
		Assert.assertEquals("{(あいうえお,1),(かきくけこ,1)}", value.toString());
	}
	
	@Test
	public void 全角スペースでもBagで2つ返ってくること () throws IOException {

		ParseSearchTerm ps = new ParseSearchTerm();
		DataBag value = ps.get("あいうえお　かきくけこ");
		System.out.println(value.toString());
		Assert.assertEquals("{(あいうえお,1),(かきくけこ,1)}", value.toString());
	}
	
	@Test
	public void Bagでひとつ返ること () throws IOException {

		ParseSearchTerm ps = new ParseSearchTerm();
		DataBag value = ps.get("あいうえお");
		System.out.println(value.toString());
		Assert.assertEquals("{(あいうえお,1)}", value.toString());
	}
	
	@Test
	public void 空白が返ること () throws IOException {

		ParseSearchTerm ps = new ParseSearchTerm();
		DataBag value = ps.get("");
		System.out.println(value.toString());
		Assert.assertEquals("{(,1)}", value.toString());
	}
}
