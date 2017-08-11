import org.apache.spark.api.java.function.Function;

public class BackEdgesMapper implements Function<String,String>{
    public String call(String s){
        String [] parts = s.split("\\s+");
        for (int i=3;i<parts.length; i++) {
            if (!parts[i].equals("."))
                parts[2] = parts[2].concat(parts[i]);
        }
        return  parts[2] + " <> " + parts[0];
    }
}
