package com.gsd.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataType;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.impl.logicalLayer.FrontendException;

/**
 * GetReqParam implements eval function to get request parameter from Apache log.
 * ApacheログのリクエストURLから指定したパラメータを取得する
 * <div>
 * Example:<div><code>
 *      A = load 'mydata' using PigStrage() as request;<br />
 *      B = foreach A generate GetReqParam(request,'url');<br /></code></div>
 * <ul><li>第一引数：リクエストURL</li>
 * <li>第二引数：取り出したいパラメータ</li><ul>
 * </div>
 * @author ota
 * 
 */
public class GetReqParam extends EvalFunc<String>
{

	static final String AND = "&";
	static final String EQUAL = "=";
	static final String BLANK = "";
	static final String REGEX_AND = "&.*";
	
	/**
	 * Method invoked on every tuple during foreach evaluation
	 * @param input tuple; first column is assumed to have the column to convert
	 * @exception java.io.IOException
	 */
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() < 2)
			return null;
		try{
			String request = (String)input.get(0);
			String param = (String)input.get(1);
			return get(request,param);
		}catch(Exception e){
			log.warn("Failed to process input; error - " + e.getMessage());
			return null;
		}
	}

	/**
	 * リクエストURIからパラメータを取得するメソッド 
	 * @param リクエストURL,url等のリクエストパラメータ
	 * @exception java.io.IOException
	 */
	public String get(String request,String param) throws IOException{
		
		String[] split = request.split("&"+param+"=");
		String value = split[1].replaceAll(REGEX_AND,BLANK);

		return value;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.CHARARRAY));
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
	 */
	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		Schema s = new Schema();
		s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		funcList.add(new FuncSpec(this.getClass().getName(), s));
		return funcList;
	}
}
