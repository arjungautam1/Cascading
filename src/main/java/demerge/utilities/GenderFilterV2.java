/**
 * Created By Arjun Gautam
 * Date :21/09/2021
 * Time :14:13
 * Project Name :CustomFilterCascading
 */
package demerge.utilities;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

public class GenderFilterV2 extends BaseOperation implements Filter {
    private String filterInValue;
    public GenderFilterV2(String filterInValue){
        this.filterInValue=filterInValue;
    }

    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry tupleEntry=filterCall.getArguments();
        return !tupleEntry.getString("gender").equals(filterInValue);
    }
}
