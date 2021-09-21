/**
 * Created By Arjun Gautam
 * Date :21/09/2021
 * Time :14:31
 * Project Name :CustomFilterCascading
 */
package dob;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;

public class Main {
    public static void main(String[] args) {
        /*Calculate and store age on the basis of provided DOB in source file.*/
        String sourcePath = "src/main/resources/store_age/dob_src.txt";
        String sinkPath = "src/main/resources/store_age/dob_sink.txt";

        Tap sourceTap = new FileTap(new TextDelimited(true, ",", "\""), sourcePath);
        Tap sinkTap = new FileTap(new TextDelimited(true, "|"), sinkPath, SinkMode.REPLACE);

        Pipe pipe = new Pipe("dobPipe");
        pipe = new Each(pipe, new Insert(new Fields("age"), ""), Fields.ALL);
        pipe = new Each(pipe, new Fields("dob", "age"), new AgeCalculatorFunction(), Fields.REPLACE);

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(pipe, sinkTap);
        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();

        System.out.println("Process completed. Please visit \n " + sinkPath);

    }
}
