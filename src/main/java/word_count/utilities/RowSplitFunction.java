/**
 * Created By Arjun Gautam
 * Date :21/09/2021
 * Time :12:22
 * Project Name :CustomFilterCascading
 */
package word_count.utilities;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

public class RowSplitFunction extends BaseOperation implements Function {
    public RowSplitFunction() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, FunctionCall functionCall) {
        TupleEntry tupleEntry = functionCall.getArguments();
        TupleEntry tuple = new TupleEntry(tupleEntry);
        String[] row = tuple.getString("line").split("\\s+");
        for (String word : row) {
            word = word.replaceAll("[^a-zA-Z0-9]", ""); //removing special characters
            tuple.setString("line", word);
            functionCall.getOutputCollector().add(tuple);
        }
    }
}
