import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class VidMapper2 implements PairFunction<String, String, String> {
    static long vid=1;
    public scala.Tuple2<String,String> call(String s){
               return new scala.Tuple2<String,String>(s, String.valueOf(vid++));
        }
    public void resetVid(){
        vid = 1;
    }
}
