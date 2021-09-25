package sum_amount;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

/**
 * Created By Arjun Gautam
 * Date :25/09/2021
 * Time :11:32
 * Project Name :CustomFilterCascading
 */

public class sum_amount {
    public static void main(String[] args) {
        /*
        * From a record of mbr_id and paid_amount fields,
        * if there are multiple records of same mbr_id, then sum up the paid amount.
        * */

        String sourcePath="src/main/resources/sum_amount/sum_source.txt";
        String sinkPath="src/main/resources/sum_amount/sum_sink.txt";

        Tap sourceTap=new FileTap(new TextDelimited(true,";"),sourcePath);
        Tap sinkTap=new FileTap(new TextDelimited(true,";"),sinkPath, SinkMode.REPLACE);

        Pipe pipe=new Pipe("sumUpAmount");
        Fields groupingFields=new Fields("mbr_id");
        pipe =new GroupBy(pipe,groupingFields);
        pipe=new Every(pipe,new Fields("paid_amount"),new SummingBuffer(),Fields.REPLACE);

        FlowDef flowDef=FlowDef.flowDef()
                .addSource(pipe,sourceTap)
                .addTailSink(pipe,sinkTap);

        Flow flow=new LocalFlowConnector().connect(flowDef);
        flow.complete();

        System.out.println("Process completed. \n Please visit :\n"+sinkPath);





    }
}
