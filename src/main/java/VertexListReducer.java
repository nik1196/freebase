import org.apache.spark.api.java.function.Function2;

public class VertexListReducer implements Function2<String,String,String> {
    public String call(String a, String b){
        return a+b;
    }
}
