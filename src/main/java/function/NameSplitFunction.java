/**
 * Created By Arjun Gautam
 */
package function;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class NameSplitFunction extends BaseOperation implements Function {
    public NameSplitFunction() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tupleEntry = functionCall.getArguments();
        TupleEntry tuple = new TupleEntry(tupleEntry);
        String[] name = tupleEntry.getString("full_name").split("\\s+");

        /*
        \\s - matches single whitespace character
        \\s+ - matches sequence of one or more whitespace characters.
        */

        //logic applied
        tuple.setString("first_name", name[0]);
        if (name.length == 2) {
            tuple.setString("last_name", name[1]);
        } else if (name.length == 3) {
            tuple.setString("middle_name", name[1]);
            tuple.setString("last_name", name[2]);
        }
        functionCall.getOutputCollector().add(tuple);  //sends resulting tuple to the pipe stream

    }
}
