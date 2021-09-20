package filter;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;

public class Main {
    public static void main(String[] args) {
//        1.Define source tap and sink tap

        String sourcePath = "src/main/resources/filter/filter_source.txt";
        String sinkPath = "src/main/resources/filter/filter_sink.txt";

        Tap sourceTap = new FileTap(new TextDelimited(true, ";"), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(true, "|"), sinkPath, SinkMode.REPLACE);

//        3.Now create a pipe on which above operation will be performed

        Pipe pipe = new Pipe("genderFilterPipe");
        pipe = new Each(pipe, new GenderFilter());

//        4.Finally, we create flow and execute the operation
        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);

        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();


    }
}
