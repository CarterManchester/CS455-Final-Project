
package svi_prison_analysis;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.*;
// import static org.apache.spark.sql.functions.*;
// import static org.apache.spark.sql.functions.split;
import java.util.*;

import svi_prison_analysis.States.*;

public class Main {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("Investigating Correlations Between Social Vulnerability and Prison Populations")
            .master("local[*]") // IMPORTANT!! LOCAL MODE! Comment out (or change?) when testing on HDFS cluster!
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        spark.conf().set("spark.sql.debug.maxToStringFields", "200"); //NOTE: This is so it prints strings without size warnings. Change back if needed!!

        // NOTE: quick spark test to see if spark is running (print 0-10):
        // Dataset<Row> df = spark.range(0, 10).toDF("value");
        // df.show();

        
        //========= Individual States =========
        State colorado = new Colorado(spark);
        Illinois illinois = new Illinois(spark);
        colorado.runSVI(); // NOTE: can comment out if this is too annoying! just shows individual states.
        illinois.runSVI();

        // ========= Joined States =========
        mostVulnerableCountiesAcrossStates(10, colorado, illinois);

        spark.stop();
    }


    /**
     * A method for joining states and data. Change or copy or add more as we need!
     * HOWEVER I dont think this is completely justified since its based on vastly different population sizes right?
     * 
     * @param n number of counties.
     * @param states each state included in the combined data of states.
     */
    private static void mostVulnerableCountiesAcrossStates(int n, State... states) {//no way this 'State... states' works bro... why didnt i know about this before??
        List<State> stateList = Arrays.asList(states);
        Dataset<Row> combinedData = null;

        for (State state : stateList){
            Dataset<Row> temp = state.getData().select(
                col("STATE"),
                col("COUNTY"),
                col("FIPS"),
                col("RPL_THEMES"),
                col("RPL_THEME1"),
                col("RPL_THEME3")
            );
            
            if(combinedData == null){
                combinedData = temp;
            }
            else{
                combinedData = combinedData.unionByName(temp); 
            }
        }

        System.out.println("\n\n\n==============================================================================================");
        System.out.println("\nTop " + n + " Most Vulnerable Counties Across All Provided States (RPL_THEMES in descending order)");
        System.out.println("==============================================================================================\n");

        combinedData.orderBy(col("RPL_THEMES").desc()).show(n, false);
    }
}
