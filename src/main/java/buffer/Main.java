/**
 * Created By Arjun Gautam
 * Date :20/09/2021
 * Time :14:18
 * Project Name :CustomFilterCascading
 */
package buffer;

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

public class Main {
    public static void main(String[] args) {
        String sourcePath = "src/main/resources/buffer/buffer_source.txt";
        String sinkPath = "src/main/resources/buffer/buffer_sink.txt";

        Tap sourceTap = new FileTap(new TextDelimited(true, ";"), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(true, ";"), sinkPath, SinkMode.REPLACE);

        Pipe pipe = new Pipe("bufferExamplePipe");
        Fields groupingFields = new Fields("id");
        Fields sortFields = new Fields("paid_amount");

        sortFields.setComparator("paid_amount", new CustomComparator() {
            @Override
            public int compare(Object o1, Object o2) {
                return Integer.valueOf((String) o1).compareTo(Integer.valueOf((String) o2));
            }
        });

        pipe = new GroupBy(pipe, groupingFields, sortFields, true);
        pipe = new Every(pipe, new CustomBuffer(), Fields.REPLACE);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);

        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
    }
}

