package com.gsd.pig.udf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.EvalFunc;
import org.apache.pig.FuncSpec;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.BagFactory;
import org.apache.pig.data.TupleFactory;
import org.apache.pig.impl.logicalLayer.FrontendException;
import org.apache.pig.impl.logicalLayer.schema.Schema;
/**
 * ParseSearchTerm implements eval function to parse space delimited search term.
 * <div>Example:<div><code>
 *    A = load 'mydata' as (searchTerm:chararray);<br />
 *    B = foreach A generate ParseSearchTerm(searchTerm) as searchTermBag;<br />
 *    C = foreach B generate FLATTEN(searchTermBag);<br />
 *</code></div></div>
 * @author ota
 * 
 */
public class ParseSearchTerm extends EvalFunc<DataBag>{

	private static final String SPACE = " ";
	private static final String EM_SPACE = "　";
	private static final int ZERO = 0;
	private static final int ONE = 1;
	private static final int TWO = 2;
	
	public DataBag exec(Tuple input) throws IOException {
		if (input == null || input.size() < 1)
			return null;
		try{
			String searchTerm = (String)input.get(0);
			return get(searchTerm);
		}catch(Exception e){
			log.warn("Failed to process input; error - " + e.getMessage());
			return null;
		}
	}

	public DataBag get(String searchTerm) throws IOException{
		DataBag stBag = BagFactory.getInstance().newDefaultBag();
		String[] searchTermArray = searchTerm.replaceAll(EM_SPACE, SPACE).split(SPACE);
		for (String term : searchTermArray) {
			Tuple stTuple = TupleFactory.getInstance().newTuple(TWO);
			stTuple.set(ZERO, term);
			stTuple.set(ONE, ONE);
			stBag.add(stTuple);
		}
		return stBag;
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.BAG));
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
