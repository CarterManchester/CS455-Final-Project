package svi_prison_analysis;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
// import static org.apache.spark.sql.functions.split;
// import java.util.*;


/**
 * This is the super class for any given US state. It finds the dataset JSON file, and then runs Spark operations
 * in order to sort data the way we need it to be sorted for every state.
 */
public abstract class State {
    private final SparkSession spark;
    private Dataset<Row> data;
    protected abstract String getStateName();

    public State(SparkSession spark) {
        this.spark = spark;
    }


    public void runSVI() { // main method for a given US State
        String state = getStateName();
        System.out.println("\n\n\n==============================================================================================");
        System.out.println("| " + state + " SVI Data");
        System.out.println("==============================================================================================\n");

        loadData();

        mostVulnerableCounties(10);
        leastVulnerableCounties(10);
        highestGroupQuarters(10);
        highestSocioeconomicVulnerability(10);
        highestMinorityVulnerability(10);
    }

    private void loadData() {
        String state = getStateName();
        String basePath = "src/main/resources";
        String pathToStateData = basePath + "/svi_county_GISJOIN." + state + ".raw_data.json";

        this.data = spark.read()
            .option("multiLine", true) // have to do this cause basically the file is a JSON array instead of a csv like HW4
            .json(pathToStateData);
        // .option("path", pathToStateData)
        // .load();
        data = data.cache();

        System.out.println("  - Row count of " + state + " SVI data: " + data.count());
        // System.out.println("==============================================================================================\n ");
        System.out.println("----------------------------------------------------------------------------------------------");
    }






    // ==================== sorting data below! ====================

    // see README for variable meanings!!

    /**
     * Sort by overall SVI percentile (RPL_THEMES) in descending order.
     * 
     * @param n number of counties.
     */
    private void mostVulnerableCounties(int n) {
        System.out.println("\nTop " + n + " Most Vulnerable Counties (RPL_THEMES in descending order)");
        data.select(
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
     * 
     * @param n number of counties.
     */
    private void leastVulnerableCounties(int n) {
        System.out.println("\nTop " + n + " Least Vulnerable Counties (RPL_THEMES in descending order)");
        data.select(
            col("STATE"),
            col("COUNTY"),
            col("FIPS"),
            col("RPL_THEMES"),
            col("RPL_THEME1"),
            col("RPL_THEME3")
        ).orderBy(col("RPL_THEMES").asc()).show(n, false);
    }

    /**
     * Sort by EP_GROUPQ (percentage of persons in group quarters) - this is super
     * relevant for prisons!
     * 
     * Looks like it includes prisons, jails, college dorms, group homes, etc. and
     * can show us which counties
     * have large prison facilities and the number of prisons per county.
     * 
     * @param n number of counties.
     */
    private void highestGroupQuarters(int n) {
        System.out.println("\nTop " + n + " Counties by Percentage of Persons in Group Quarters (EP_GROUPQ desc)");
        data.select(
            col("STATE"),
            col("COUNTY"),
            col("FIPS"),
            col("EP_GROUPQ"),
            col("RPL_THEMES")
        ).orderBy(col("EP_GROUPQ").desc()).show(n, false);
    }

    /**
     * Sort by Socioeconomic Status (RPL_THEME1)
     * 
     * @param n number of counties.
     */
    private void highestSocioeconomicVulnerability(int n) {
        System.out.println("\nTop " + n + " Counties by Socioeconomic Vulnerability (RPL_THEME1 in descending order)");
        data.select(
            col("STATE"),
            col("COUNTY"),
            col("FIPS"),
            col("RPL_THEME1"),
            col("EP_POV"), // poverty
            col("EP_UNEMP"), // unemployment
            col("EP_NOHSDP") // no high school diploma
        ).orderBy(col("RPL_THEME1").desc()).show(n, false);
    }

    /**
     * Sort by Minority and Language (RPL_THEME3)
     * 
     * @param n number of counties.
     */
    private void highestMinorityVulnerability(int n) {
        System.out.println("\nTop " + n + " Counties by Minority/Language Vulnerability (RPL_THEME3 in descending order)");
        data.select(
                col("STATE"),
                col("COUNTY"),
                col("FIPS"),
                col("RPL_THEME3"),
                col("EP_MINRTY"), // minority %
                col("EP_LIMENG") // liimited english
        ).orderBy(col("RPL_THEME3").desc()).show(n, false);
    }

}
