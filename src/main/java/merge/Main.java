/**
 * Created By Arjun Gautam
 * Date :20/09/2021
 * Time :20:17
 * Project Name :CustomFilterCascading
 */
package merge;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Merge;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;

public class Main {
    public static void main(String[] args) {


        String source1Path = "src/main/resources/merge/merge_source1.txt";
        String source2Path = "src/main/resources/merge/merge_source2.txt";
        String sinkPath = "src/main/resources/merge/merge_sink.txt";

        Tap source1Tap = new FileTap(new TextDelimited(true, ";"), source1Path);
        Tap source2Tap = new FileTap(new TextDelimited(true, "|"), source2Path);
        Tap sinkTap = new FileTap(new TextDelimited(true, "\t"), sinkPath, SinkMode.REPLACE);

        Pipe pipe1 = new Pipe("pipe1");
        Pipe pipe2 = new Pipe("Pipe2");

        Pipe finalPipe = new Merge(pipe1, pipe2);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe1, source1Tap)
                .addSource(pipe2, source2Tap)
                .addTailSink(finalPipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }
}
