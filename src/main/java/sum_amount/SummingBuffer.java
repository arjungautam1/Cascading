/**
 * Created By Arjun Gautam
 * Date :25/09/2021
 * Time :11:45
 * Project Name :CustomFilterCascading
 */
package sum_amount;

import cascading.flow.FlowProcess;
import cascading.operation.Aggregator;
import cascading.operation.BaseOperation;
import cascading.operation.Buffer;
import cascading.operation.BufferCall;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

import java.util.Iterator;

public class SummingBuffer extends BaseOperation implements Buffer {
    public SummingBuffer(){
        super(Fields.ARGS);
    }

    @Override
    public void operate(FlowProcess flowProcess, BufferCall bufferCall) {
        Iterator<TupleEntry> tupleEntryIterator=bufferCall.getArgumentsIterator();
        TupleEntry firstTuple=new TupleEntry(tupleEntryIterator.next());
        int sum=firstTuple.getInteger("paid_amount");
        while (tupleEntryIterator.hasNext()){
            sum=sum+tupleEntryIterator.next().getInteger("paid_amount");
        }
        System.out.println(sum);
        firstTuple.setInteger("paid_amount",sum);
        bufferCall.getOutputCollector().add(firstTuple);
    }
}
