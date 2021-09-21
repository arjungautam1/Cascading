/**
 * Created By Arjun Gautam
 * Date :21/09/2021
 * Time :14:37
 * Project Name :CustomFilterCascading
 */
package dob;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.time.Year;

public class AgeCalculatorFunction extends BaseOperation implements Function {

    public AgeCalculatorFunction() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tupleEntry = functionCall.getArguments();
        TupleEntry tuple = new TupleEntry(tupleEntry);
        String dob = tuple.getString("dob");
        tuple.setInteger("age", getAge(dob));
        functionCall.getOutputCollector().add(tuple);
    }

    private int getAge(String dob) {

        return Year.now().compareTo(Year.parse(dob.substring(0, 4)));
    }

}
