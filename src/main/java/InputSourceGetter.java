import org.apache.spark.api.java.function.Function;
import scala.Tuple2;
import scala.Tuple3;

public class InputSourceGetter implements Function<Tuple2<String, String>, String> {
    public String call(Tuple2<String, String> s){
        return s._1();
    }
}
