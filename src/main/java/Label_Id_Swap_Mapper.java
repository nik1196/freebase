import com.google.common.base.Optional;
import org.apache.spark.api.java.function.PairFunction;

public class Label_Id_Swap_Mapper implements PairFunction<scala.Tuple2<String, scala.Tuple2<scala.Tuple2<Long,Optional<String>>,String>>, Long, scala.Tuple2<String,String>> {
    public scala.Tuple2<Long, scala.Tuple2<String, String>>call(scala.Tuple2<String, scala.Tuple2<scala.Tuple2<Long, com.google.common.base.Optional<String>>,String>> s){
        String replaceOptional = "label:String:"+s._1()+"$";
        try{
            replaceOptional+=s._2()._1()._2().get();
        }
            catch(Exception e){}
        return new scala.Tuple2<Long, scala.Tuple2<String, String>> (s._2()._1()._1(), new scala.Tuple2<String,String>(replaceOptional,s._2()._2()));
    }
}
