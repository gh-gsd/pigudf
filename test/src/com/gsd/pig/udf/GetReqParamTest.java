package com.gsd.pig.udf;

import java.io.IOException;
import junit.framework.Assert;
import org.junit.Test;

public class GetReqParamTest {

	@Test
	public void test_param01が取得できること () throws IOException {
		String request = "http://www.google.co.jp/imgres?q=%E3%81%93%E3%81%91%E3%81%97&um=1&hl=ja&rlz=1T4ADFA_jaSE440SE440&biw=1018&bih=544&tbm=isch&tbnid=CRkBHUgr8omzpM:&url=http://newsbuzz.jp/&docid=QMKOEzP6QtC--M&imgurl=http://www.gizmodo.jp/upload_files/100905kokeshi1.jpg&w=450&h=450&ei=ztmZToG9OoKF4gSn1v3yAw&zoom=1&param01=aaa&param02=bbb";
		String param = "param01";

		GetReqParam gp = new GetReqParam();
		String value = gp.get(request, param);
		System.out.println(value);
		Assert.assertEquals("aaa", value);
	}

	@Test
	public void test_urlが取得できること () throws IOException {
		String request = "http://www.google.co.jp/imgres?q=%E3%81%93%E3%81%91%E3%81%97&um=1&hl=ja&rlz=1T4ADFA_jaSE440SE440&biw=1018&bih=544&tbm=isch&tbnid=CRkBHUgr8omzpM:&url=http://newsbuzz.jp/&docid=QMKOEzP6QtC--M&imgurl=http://www.gizmodo.jp/upload_files/100905kokeshi1.jpg&w=450&h=450&ei=ztmZToG9OoKF4gSn1v3yAw&zoom=1&param01=aaa&param02=bbb";
		String param = "url";
		GetReqParam gp = new GetReqParam();
		String value = gp.get(request, param);
		Assert.assertEquals("http://newsbuzz.jp/", value);
	}

	@Test
	public void test_param02が取得できること () throws IOException {
		String request = "http://www.google.co.jp/imgres?q=%E3%81%93%E3%81%91%E3%81%97&um=1&hl=ja&rlz=1T4ADFA_jaSE440SE440&biw=1018&bih=544&tbm=isch&tbnid=CRkBHUgr8omzpM:&url=http://newsbuzz.jp/&docid=QMKOEzP6QtC--M&imgurl=http://www.gizmodo.jp/upload_files/100905kokeshi1.jpg&w=450&h=450&ei=ztmZToG9OoKF4gSn1v3yAw&zoom=1&param01=aaa&param02=bbb";
		String param = "param02";
		GetReqParam gp = new GetReqParam();
		String value = gp.get(request, param);
		Assert.assertEquals("bbb", value);
	}
}
