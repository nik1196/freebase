import org.apache.spark.api.java.function.Function;

public class VidMapper2 implements Function<scala.Tuple3<String,String,String>, scala.Tuple2<String,String>>{
    static long vid=1;
    public scala.Tuple2<String,String> call(scala.Tuple3<String,String,String> s){
               return new scala.Tuple2<String,String>(s._1(), String.valueOf(vid++));
        }
}
