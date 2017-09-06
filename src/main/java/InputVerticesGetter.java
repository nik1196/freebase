import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

public class InputVerticesGetter implements PairFunction<Tuple2<String,String>, String,String> {
    public scala.Tuple2<String, String> call(scala.Tuple2<String, String> s){
        String [] parts = s._2().split("\\s+");
        return new scala.Tuple2<String, String>(s._1(), parts[1]);
    }
}
