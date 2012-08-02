package com.gsd.pig.udf;

import java.io.IOException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * ParseBag implements eval function to parse DataType.BAG.
 * <div>Example:<div><code>
 *    A = load 'mydata' as (id:chararray,product_id:chararray,count:int);<br />
 *    B = group A by id;<br />
 *    C = foreach B generate ParseBag(group,"|","_");<br />
 * </code></div>
 * <ul><li>第一引数：Bag data</li>
 * <li>第二引数：Bag delimiter</li>
 * <li>第三引数：Tuple delimiter</li><ul>
 * </div>
 * @author ota
 * 
 */
public class ParseBag extends EvalFunc<String>{
	
	public String exec(Tuple input) throws IOException {
		if (input == null || input.size() < 3)
			return null;
		try{
			DataBag segmentInfo = (DataBag)input.get(0);
			String bagDelim = (String)input.get(1);
			String tupleDelim = (String)input.get(2);
			return get(segmentInfo, bagDelim, tupleDelim);
		}catch(Exception e){
			log.warn("Failed to process input; error - " + e.getMessage());
			return null;
		}
	}

	public String get(DataBag segmentInfo, String bagDelim, String tupleDelim) throws IOException{
		
		StringBuilder sb = new StringBuilder();
		for (Iterator<Tuple> ite = segmentInfo.iterator() ; ite.hasNext();){
			Tuple data = ite.next();
			sb.append(data.toDelimitedString(tupleDelim) + bagDelim);
		}
		
		String result = sb.substring(0, sb.length() - 1);
		
		return result;
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
		s.add(new Schema.FieldSchema(null, DataType.BAG));
		s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		s.add(new Schema.FieldSchema(null, DataType.CHARARRAY));
		funcList.add(new FuncSpec(this.getClass().getName(), s));
		return funcList;
	}
}
