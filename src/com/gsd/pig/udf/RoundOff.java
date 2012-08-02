package com.gsd.pig.udf;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * RoundOff implements eval function to rounding off a double value.
 * <div>Example:<div><code>
 *	  A = load 'mydata' as (value:double);<br />
 *	  B = foreach A generate RoundOff(value);<br /></code></div></div>
 * @author ota
 * 
 */
public class RoundOff extends EvalFunc<Double>{

	static final int ROUND_COUNT = 5;
	
	public Double exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return 0.0;
		try{
			Double number = (Double)input.get(0);
			return get(number);
		}catch(Exception e){
			log.warn("Failed to process input; error - " + e.getMessage());
			return 0.0;
		}
	}

	public double get(Double number) throws IOException{
		
		BigDecimal bd = new BigDecimal(String.valueOf(number));
		double value = bd.setScale(ROUND_COUNT, BigDecimal.ROUND_HALF_UP).doubleValue();
		
		return value;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
	}

	/* (non-Javadoc)
	 * @see org.apache.pig.EvalFunc#getArgToFuncMapping()
	 */
	@Override
	public List<FuncSpec> getArgToFuncMapping() throws FrontendException {
		List<FuncSpec> funcList = new ArrayList<FuncSpec>();
		Schema s = new Schema();
		s.add(new Schema.FieldSchema(null, DataType.DOUBLE));
		funcList.add(new FuncSpec(this.getClass().getName(), s));
		return funcList;
	}
}
