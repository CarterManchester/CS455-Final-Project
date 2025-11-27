package svi_prison_analysis;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.split;
import java.util.*;

public class Colorado {
    private final SparkSession spark;
    private Dataset<Row> coloradoSVI;

    public Colorado (SparkSession spark){
        this.spark = spark;
    }



    public void runSVI(){ //main method for Colorado
        loadData();
        System.out.println(
                "\n==========================================\n Colorado SVI Data \n==========================================\n");
        printSchemaAndSample(); //prints schema and first 5 rows (first 5 rows was annoying so I commented out - C)

        mostVulnerableCounties(10);
        leastVulnerableCounties(10);
        highestGroupQuarters(10);
    }

    private void loadData(){
        String basePath = "src/main/resources";
        String coloradoPath = basePath + "/svi_county_GISJOIN.Colorado.raw_data.json";

        this.coloradoSVI = spark.read()
            .option("multiLine", true) // have to do this cause basically the file is a JSON array instead of a csv like HW4
            .json(coloradoPath);
            // .option("path", coloradoPath)
            // .load();
        coloradoSVI = coloradoSVI.cache();

        System.out.println("Row count of Colorado SVI data: " + coloradoSVI.count());
    }

    private void printSchemaAndSample(){
        System.out.println(" -- Colorado SVI Schema: --");
        coloradoSVI.printSchema();
        // System.out.println("\n=== First 5 Rows ===");     //commented out cause its annoying lol
        // coloradoSVI.show(5, false);
    }

    
    
    //==================== sorting data below! ====================

    //see README for variable meanings!!

    /**
     * Sort by overall SVI percentile (RPL_THEMES) in descending order.
     * @param n number of counties.
     */
    private void mostVulnerableCounties(int n){
        System.out.println("\nTop " + n + " Most Vulnerable Counties (RPL_THEMES in descending order)");
        coloradoSVI.select(
            col("STATE"),
            col("COUNTY"),
            col("FIPS"),
            col("RPL_THEMES"),
            col("RPL_THEME1"),
            col("RPL_THEME3")
        ).orderBy(col("RPL_THEMES").desc()).show(n, false);
    }

    /**
     * Sort by overall SVI percentile (RPL_THEMES) in ascending order.
     * @param n number of counties.
     */
    private void leastVulnerableCounties(int n){
        System.out.println("\nTop " + n + " Least Vulnerable Counties (RPL_THEMES in descending order)");
        coloradoSVI.select(
            col("STATE"),
            col("COUNTY"),
            col("FIPS"),
            col("RPL_THEMES"),
            col("RPL_THEME1"),
            col("RPL_THEME3")).orderBy(col("RPL_THEMES").asc()).show(n, false);
    }

    /**
     * Sort by EP_GROUPQ (percentage of persons in group quarters) - this is super relevant for prisons!
     * 
     * Looks like it includes prisons, jails, college dorms, group homes, etc. and can show us which counties
     * have large prison facilities and the number of prisons per county.
     * 
     * @param n number of counties.
     */
    private void highestGroupQuarters(int n){
        System.out.println("\nTop " + n + " Counties by Percentage of Persons in Group Quarters (EP_GROUPQ desc)");
        coloradoSVI.select(
            col("STATE"),
            col("COUNTY"),
            col("FIPS"),
            col("EP_GROUPQ"),
            col("RPL_THEMES")
        ).orderBy(col("EP_GROUPQ").desc()).show(n, false);
    }



}
