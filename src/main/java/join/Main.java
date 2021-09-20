/**
 * Created By Arjun Gautam
 * Date :20/09/2021
 * Time :16:34
 * Project Name :CustomFilterCascading
 */
package join;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.CoGroup;
import cascading.pipe.Pipe;
import cascading.pipe.assembly.Discard;
import cascading.pipe.joiner.LeftJoin;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class Main {
    public static void main(String[] args) {
        String source1Path = "src/main/resources/join/join_source1.txt";
        String source2Path = "src/main/resources/join/join_source2.txt";
        String sinkPath = "src/main/resources/join/join_sink.txt";

        Tap source1Tap=new FileTap(new TextDelimited(true,"|"),source1Path);
        Tap source2Tap=new FileTap(new TextDelimited(true,";"),source2Path);

        Tap sinkTap=new FileTap(new TextDelimited(true,"|"),sinkPath,SinkMode.REPLACE);


        Pipe pipe1 = new Pipe("pipe1");
        Pipe pipe2 = new Pipe("pipe2");

        Fields commonFields = new Fields("id");
        Fields declaredFields = new Fields("id", "name","phone" ,"id2", "address");

        Pipe finalPipe = new CoGroup(
                pipe1,
                commonFields,
                pipe2,
                commonFields,
                declaredFields,
                new LeftJoin()
        );
        finalPipe = new Discard(finalPipe, new Fields("id2"));

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe1, source1Tap)
                .addSource(pipe2, source2Tap)
                .addTailSink(finalPipe, sinkTap);
        Flow flow=new LocalFlowConnector().connect(flowDef);
        flow.complete();

        System.out.println("Process completed. \n Please visit :\n"+sinkPath);
    }
}
