
package cs455_final_project;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.split;
import java.util.*;

public class Main {
    public String getGreeting() {
        return "Hello World!";
    }

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .appName("CS455 Project")
            .master("local[*]") // IMPORTANT!! LOCAL MODE! Comment out (or change?) when testing on HDFS cluster!
            .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");

        // NOTE: Below is a quick spark test (should work and print 0-10):
        // Dataset<Row> df = spark.range(0, 10).toDF("value");
        // df.show();

        
        Colorado colorado = new Colorado(spark);
        colorado.runSVI();

        spark.stop();
    }
}
