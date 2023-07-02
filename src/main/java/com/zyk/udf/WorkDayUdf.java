package com.zyk.udf;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Objects;

@Description(
name="workday",
value="_FUNC_(start_dt, end_dt) - 返回两个日期间所经过的工作日数量,不包括起始当天,接受 yyyy-MM-dd格式的string 或者 timestamp",
extended = "Example:\n"
        + " > SELECT _FUNC_('2023-06-20', '2023-06-25'); \n"
)
public class WorkDayUdf extends GenericUDF {
//    final static Logger logger = LoggerFactory.getLogger(WorkDayUdf.class);
    TimestampObjectInspector timestampOI;
    StringObjectInspector stringOI;
    public static List<String> workDayList = new ArrayList<>();
//    public List<String> workDayList = new ArrayList<>();
//
    static{
        try {
            String line = "";
            BufferedReader f = new BufferedReader(new InputStreamReader(Objects.requireNonNull(WorkDayUdf.class.getClassLoader().getResourceAsStream("workdays.csv"))));

            while( (line = f.readLine()) != null){
                String[] cols = line.split("\t");
                if("1".equals(cols[1])){
                    workDayList.add(cols[0]);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
        // 检查参数数量
        if(arguments.length != 2){
            throw new UDFArgumentLengthException("需要传入2个参数！");
        }

        // 检查是否timestamp或者string
        ObjectInspector a = arguments[0];
        ObjectInspector b = arguments[1];
        if ( a instanceof TimestampObjectInspector && b instanceof TimestampObjectInspector) {
            timestampOI = (TimestampObjectInspector) a;
        } else if (a instanceof StringObjectInspector && b instanceof StringObjectInspector) {
            stringOI = (StringObjectInspector) a;
        } else {
            throw new UDFArgumentException("参数类型为 timestamp 或者 yyyy-MM-dd格式string");
        }
//        logger.warn("完成init!");
        // 指定返回结果的类型解释器
        return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
    }


    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        // 解析参数值
        String startStr, endStr = "";
        Date startDt, endDt;
//        logger.warn("进入evaluate!");
        if(timestampOI != null){
            startStr = timestampOI.getPrimitiveJavaObject(arguments[0].get()).toString().substring(0,10);
            endStr = timestampOI.getPrimitiveJavaObject(arguments[1].get()).toString().substring(0,10);
        } else {
            startStr = stringOI.getPrimitiveJavaObject(arguments[0].get()).substring(0,10);
            endStr = stringOI.getPrimitiveJavaObject(arguments[1].get()).substring(0,10);
        }

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        int workDayCount = 0;

        try {
            startDt = sdf.parse(startStr);
            endDt = sdf.parse(endStr);
            if(endDt.after(sdf.parse("2023-12-31"))){
                throw new UDFArgumentException("endDt必须小于2024-01-01");
            }

            for(String day : workDayList){
                Date workDay = sdf.parse(day);
                if(workDay.after(startDt) && workDay.compareTo(endDt) <= 0){
                    workDayCount++;
                }
            }
        } catch (ParseException e) {
            throw new UDFArgumentException("参数类型为 timestamp 或者 yyyy-MM-dd格式string");
        }

        return workDayCount;
    }

    @Override
    public String getDisplayString(String[] children) {
        return "比较dt1和dt2之间有多少个工作日";
    }
}
