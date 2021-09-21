/**
 * Created By Arjun Gautam
 * Date :21/09/2021
 * Time :11:35
 * Project Name :CustomFilterCascading
 */
package word_count;

import cascading.flow.Flow;
import cascading.flow.FlowDef;
import cascading.flow.local.LocalFlowConnector;
import cascading.operation.Insert;
import cascading.pipe.Each;
import cascading.pipe.Every;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.scheme.local.TextDelimited;
import cascading.scheme.local.TextLine;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tap.local.FileTap;
import cascading.tuple.Fields;
import word_count.utilities.GroupCountBuffer;
import word_count.utilities.RowSplitFunction;

public class Main {
    public static void main(String[] args) {
        String sourcePath="src/main/resources/word_count/shape_of_you.txt";
        String sinkPath="src/main/resources/word_count/ed_sheeran.txt";

        Tap sourceTap=new FileTap(new TextLine(new Fields("line")),sourcePath);
        Tap sinkTap=new FileTap(new TextDelimited(new Fields("line","count"),"|"),sinkPath, SinkMode.REPLACE);

        Pipe wcPipe=new Pipe("wcPipe");
        wcPipe=new Each(wcPipe,new Insert(new Fields("count"),"1"),Fields.ALL);
        wcPipe=new Each(wcPipe,new Fields("line"),new RowSplitFunction(),Fields.REPLACE);
        wcPipe=new GroupBy(wcPipe,new Fields("line"));

        wcPipe=new Every(wcPipe,new Fields("count"),new GroupCountBuffer(),Fields.REPLACE);

        FlowDef flowDef=FlowDef
                .flowDef()
                .addSource(wcPipe,sourceTap)
                .addTailSink(wcPipe,sinkTap);
        Flow flow=new LocalFlowConnector().connect(flowDef);
        flow.complete();

        System.out.println("Process completed. \n Please visit"+sinkPath);



    }
}
