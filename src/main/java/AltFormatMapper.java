import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class AltFormatMapper implements Function<Tuple2<String, Tuple2<Long,Optional<String>>>,String> {
    public String call(scala.Tuple2<String, scala.Tuple2<Long,Optional<String>>> s) {
        StringBuilder stringBuilder = new StringBuilder(String.valueOf(s._2()._1()));
        String property = s._2()._2().isPresent()?s._2()._2().get():"@1%[label:String:" + s._1() + "]|";
        stringBuilder.append(property);
        return stringBuilder.toString();
    }
}
