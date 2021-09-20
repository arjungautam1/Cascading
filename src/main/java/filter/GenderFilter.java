package filter;

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Filter;
import cascading.operation.FilterCall;
import cascading.tuple.TupleEntry;

public class GenderFilter extends BaseOperation implements Filter {


    /*Create the Filter class which extends base operation and implements filter interface*/
    @Override
    public boolean isRemove(FlowProcess flowProcess, FilterCall filterCall) {
        TupleEntry tupleEntry = filterCall.getArguments();
        String gender = tupleEntry.getString("gender");
        return !(gender.equals("M") || gender.equals("F"));
    }
}
