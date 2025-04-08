import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
import org.apache.pig.data.DataByteArray;
public class ConcatStrings extends EvalFunc<DataByteArray> {
 @Override
 public DataByteArray exec(Tuple input) {
 if (input == null || input.size() < 2)
 return null;
 try {
 String str1 = (String) input.get(0);
 String str2 = (String) input.get(1);
 return new DataByteArray((str1 + str2).getBytes());
 } catch (Exception e) {
 return null;
 }
 }
}

