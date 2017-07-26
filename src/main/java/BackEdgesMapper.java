import org.apache.spark.api.java.function.Function;

public class BackEdgesMapper implements Function<String,String>{
    public String call(String s){
        String [] parts = s.split("\\s+");
        String k = "";
        for (int i=2;i<parts.length; i++)
            k = k.concat(parts[i]);
        parts[2] = k;
        k = parts[0] +  parts[2];
        return  k;
    }
}
