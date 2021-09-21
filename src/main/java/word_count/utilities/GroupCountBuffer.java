/**
 * Created By Arjun Gautam
 * Date :21/09/2021
 * Time :12:28
 * Project Name :CustomFilterCascading
 */
package word_count.utilities;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

public class GroupCountBuffer extends BaseOperation implements Buffer {
    public GroupCountBuffer() {
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> tupleEntryIterator = bufferCall.getArgumentsIterator();
        TupleEntry tuple = new TupleEntry(tupleEntryIterator.next());
        tuple.setString("count", counter(tupleEntryIterator).toString());
        bufferCall.getOutputCollector().add(tuple);
    }

    //count the size of grouped tuples
    private Long counter(Iterator iterator) {
        Long i = 1l; //first iteration was made while crating tuple from TupleEntry
        while (iterator.hasNext()) {
            i++;
            iterator.next();
        }
        return i;
    }
}
