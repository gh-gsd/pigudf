package com.gsd.pig.udf;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * DateTimeConverter implements eval function to convert unix time to yyyymmdd.
 * <div>Example:<div><code>
 *	  A = load 'mydata' as (unixtime:chararray);
 *	  B = foreach A generate DateTimeConverter(unixtime);</code></div></div>
 * @author ota
 * 
 */
public class DateTimeConverter extends EvalFunc<String>{
	
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return null;
		try{
			String unixTime = (String)input.get(0);
			long time = Long.parseLong(unixTime);
			return get(time);
		}catch(Exception e){
			log.warn("Failed to process input; error - " + e.getMessage());
			return null;
		}
	}

	public String get(long unixTime) throws IOException{
		
		Date date = new Date(unixTime);
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyyMMddHH");
		String value = sdf1.format(date);
		
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
		funcList.add(new FuncSpec(this.getClass().getName(), s));
		return funcList;
	}
}
