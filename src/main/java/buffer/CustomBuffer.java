/**
 * Created By Arjun Gautam
 * Date :20/09/2021
 * Time :14:18
 * Project Name :CustomFilterCascading
 */
package buffer;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

public class CustomBuffer extends BaseOperation implements Buffer {
    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> iterator=bufferCall.getArgumentsIterator();
        /*We can fetch all grouped tuples via iterator and manipulate them
        * but for now we need only the first tupleEntry*/

        bufferCall.getOutputCollector().add(new TupleEntry(iterator.next()));
        //sending first encountered tuple to the pipe|
    }
}
