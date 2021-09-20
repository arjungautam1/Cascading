/**
 * Created By Arjun Gautam
 */
package function;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class Main {
    public static void main(String[] args) {
//        1. Define source and sink

        String sourcePath = "src/main/resources/function/function_source.txt";
        String sinkPath = "src/main/resources/function/function_sink.txt";

        Tap sourceTap = new FileTap(new TextDelimited(new Fields("id", "full_name"), ";"), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(true, ";"), sinkPath, SinkMode.REPLACE);


//        3. Now let's create a pipe, upon which above Operation will bw performed.
        Pipe pipe = new Pipe("nameSplitPipe");
        Fields newFields = new Fields("first_name", "middle_name", "last_name");
        pipe = new Each(pipe, new Insert(newFields, "", "", ""), Fields.ALL);
        pipe = new Each(
                pipe,
                newFields.append(new Fields("full_name"))//full_name field appended in newFields
                , new NameSplitFunction()
                , Fields.REPLACE
        );
        pipe=new Discard(pipe,new Fields("full_name"));

//        4. Final step is to create the flow and execute operation
        FlowDef flowDef=FlowDef
                .flowDef()
                .addSource(pipe,sourceTap)
                .addTailSink(pipe,sinkTap);

        Flow flow=new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }
}
