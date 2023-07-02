import com.zyk.udf.WorkDayUdf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.junit.Test;

public class UnitTest {
    WorkDayUdf udf = new WorkDayUdf();

    @Test
    public void testConfig(){
        udf.configure(null);
        for(String workday : udf.workDayList){
            System.out.println(workday);
        }
        assert true;
    }

    @Test
    public void testParam() throws UDFArgumentException {
        ObjectInspector[] params = {PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector};
        udf.initialize(params);
        assert true;
    }

    @Test
    public void testResult() throws HiveException {
        udf.configure(null);

        ObjectInspector[] params = {PrimitiveObjectInspectorFactory.javaStringObjectInspector, PrimitiveObjectInspectorFactory.javaStringObjectInspector};
        udf.initialize(params);

        GenericUDF.DeferredObject startDt = new GenericUDF.DeferredJavaObject("2023-06-20");
        GenericUDF.DeferredObject endDt   = new GenericUDF.DeferredJavaObject("2023-06-25");
        GenericUDF.DeferredObject[] vals = {startDt, endDt};

//        int result = ((IntWritable) udf.evaluate(vals)).get();
        int result = (int) udf.evaluate(vals);
        System.out.println(result);
        if(result == 2){
            assert true;
        }else {
            assert false;
        }
    }
}
