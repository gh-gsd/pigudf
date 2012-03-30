package com.gsd.pig.udf;

import java.io.IOException;
import java.util.Iterator;

import org.apache.pig.EvalFunc;
import org.apache.pig.PigException;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.pig.backend.executionengine.ExecException;
/**
 * Variance implements eval function to calculate variance of input tuple.
 * Example:<code>
 *	  A = load 'mydata' as (data:double);
 *	  B = group A all;
 *	  C = foreach B generate Variance(A.data);
 * @author ota
 * 
 */
public class Variance extends EvalFunc<Double> {

	@Override
	public Double exec(Tuple input) throws IOException {
		try {
			Double sum = sum(input);
			if(sum == null) {
				return null;
			}
			double count = count(input);

			Double avg = null;
			if (count > 0)
				avg = new Double(sum / count);

			Double sqs = null;
			sqs = sumOfErrorsSquared(input, avg);

			Double var = sqs/(count);

			return var;
		} catch (ExecException ee) {
			throw ee;
		}
	}

	static protected long count(Tuple input) throws ExecException {
		DataBag values = (DataBag)input.get(0);
		return values.size();
	}

	static protected Double sum(Tuple input) throws ExecException, IOException {
		DataBag values = (DataBag)input.get(0);

		if(values.size() == 0) {
			return null;
		}

		double sum = 0;
		boolean sawNonNull = false;
		for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
			Tuple t = it.next();
			try{
				Double d = (Double)t.get(0);
				if (d == null) continue;
				sawNonNull = true;
				sum += d;
			}catch(RuntimeException exp) {
				int errCode = 2103;
				String msg = "Problem while computing sum of doubles.";
				exp.printStackTrace();
				throw new ExecException(msg, errCode, PigException.BUG, exp);
			}
		}

		if(sawNonNull) {
			return new Double(sum);
		} else {
			return null;
		}
	}

	static protected Double sumOfErrorsSquared(Tuple input, double avg) throws ExecException, IOException {
		DataBag values = (DataBag)input.get(0);

		if(values.size() == 0) {
			return null;
		}

		double sumsq = 0;
		boolean sawNonNull = false;
		for (Iterator<Tuple> it = values.iterator(); it.hasNext();) {
			Tuple t = it.next();
			try{
				Double d = (Double)t.get(0);
				if (d == null) continue;
				sawNonNull = true;
				double diff = d - avg;
				sumsq += (diff * diff);
			}catch(RuntimeException exp) {
				int errCode = 2103;
				String msg = "Problem while computing sum of doubles.";
				throw new ExecException(msg, errCode, PigException.BUG, exp);
			}
		}

		if(sawNonNull) {
			return new Double(sumsq);
		} else {
			return null;
		}
	}

	@Override
	public Schema outputSchema(Schema input) {
		return new Schema(new Schema.FieldSchema(null, DataType.DOUBLE));
	}
}
