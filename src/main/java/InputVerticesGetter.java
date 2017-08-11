import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class InputVerticesGetter implements Function<Tuple2<String,String>, Tuple2<String,String>> {
    public scala.Tuple2<String, String> call(scala.Tuple2<String, String> s){
        String [] parts = s._2().split("\\s+");
        return new scala.Tuple2<String, String>(s._1(), parts[1]);
    }
}
