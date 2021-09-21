/**
 * Created By Arjun Gautam
 * Date :21/09/2021
 * Time :14:06
 * Project Name :CustomFilterCascading
 */
package demerge;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import demerge.utilities.GenderFilterV2;

/*Dmerge a single pipe to mPipe and fPipe on the basis of gender "M" and "F" */
public class DeMergeMain {
    public static void main(String[] args) {
        String sourcePath = "src/main/resources/filter/filter_source.txt";
        String sinkMPath = "src/main/resources/demerge/male.txt";
        String sinkFPath = "src/main/resources/demerge/female.txt";

        Tap sourceTap = new FileTap(new TextDelimited(true, ";"), sourcePath);
        Tap sinkMTap = new FileTap(new TextDelimited(true, "|"), sinkMPath, SinkMode.REPLACE);
        Tap sinkFTap = new FileTap(new TextDelimited(true, "|"), sinkFPath, SinkMode.REPLACE);

        Pipe pipe = new Pipe("genderFilterPipe");

        Pipe mPipe = new Pipe("malePipe", pipe);
        Pipe fPipe = new Pipe("femalePipe", pipe);

        mPipe = new Each(mPipe, new Fields("gender"), new GenderFilterV2("M"));
        fPipe = new Each(fPipe, new Fields("gender"), new GenderFilterV2("F"));

        FlowDef flowDef = FlowDef
                .flowDef()
                .addSource(pipe, sourceTap)
                .addTailSink(mPipe, sinkMTap)
                .addTailSink(fPipe, sinkFTap);

        Flow flow = new LocalFlowConnector().connect(flowDef);
        flow.complete();
        System.out.println("Process completed");


    }
}
