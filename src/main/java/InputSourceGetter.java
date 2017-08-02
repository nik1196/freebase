import org.apache.spark.api.java.function.Function;
import scala.ScalaReflectionException;
import scala.Tuple3;

public class InputSourceGetter implements Function<Tuple3<String,String,String>, String> {
    public String call(Tuple3<String,String,String> s){
        return s._1();
    }
}
