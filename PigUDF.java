import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;
public class PigUDF extends EvalFunc<String> {
public String exec(Tuple tuple)throws IOException {
 if(tuple ==null) {
 return null;
 }
 String user=(String)tuple.get(0);
 String city=(String)tuple.get(1);
 int score=(Integer)tuple.get(2);
 return user+”,”+city.toUpperCase()+”,”+score;
}
}
