import eu.bitwalker.useragentutils.UserAgent;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

import java.util.ArrayList;

public class ParserUDTF extends GenericUDTF {
    private Object[] fwdObj = null;
    private PrimitiveObjectInspector nameDtlOI = null;
    private static final Integer OUT_COLS = 4;

    public StructObjectInspector initialize(ObjectInspector[] arg)
    {
        ArrayList<String> fieldNames = new ArrayList<String>();
        ArrayList<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>();

        nameDtlOI = (PrimitiveObjectInspector) arg[0];

        fieldNames.add("device");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING));

        fieldNames.add("OS");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING));

        fieldNames.add("Browser");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING));

        fieldNames.add("UA");
        fieldOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(
                PrimitiveObjectInspector.PrimitiveCategory.STRING));


        fwdObj = new Object[OUT_COLS];
        return ObjectInspectorFactory.getStandardStructObjectInspector(
                fieldNames, fieldOIs);
    }

    public void process(Object[] record) throws HiveException
    {
        String agent = nameDtlOI.getPrimitiveJavaObject(record[0]).toString();


        fwdObj[0] = UserAgent.parseUserAgentString(agent).getOperatingSystem().getDeviceType().getName();
        fwdObj[1] = UserAgent.parseUserAgentString(agent).getOperatingSystem().getGroup().name();
        fwdObj[2] = UserAgent.parseUserAgentString(agent).getBrowser().getGroup().getName();
        fwdObj[3] = UserAgent.parseUserAgentString(agent).getBrowser().getGroup().getBrowserType().getName();

        this.forward(fwdObj);

    }

    public void close()
    {

    }
}
