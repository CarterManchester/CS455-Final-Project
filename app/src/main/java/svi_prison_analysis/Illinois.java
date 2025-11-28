package svi_prison_analysis;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
// import static org.apache.spark.sql.functions.split;
// import java.util.*;

public class Illinois {
    private final SparkSession spark;
    private Dataset<Row> illinoisSVI;

    public Illinois(SparkSession spark) {
        this.spark = spark;
    }

    public void runSVI() { // main method for Illinois
        loadData();
        System.out.println(
                "\n==============================================================================================\n Illinois SVI Data \n==============================================================================================\n");

        mostVulnerableCounties(10);
        leastVulnerableCounties(10);
        highestGroupQuarters(10);
        highestSocioeconomicVulnerability(10);
        highestMinorityVulnerability(10);
    }

    private void loadData() {
        String basePath = "src/main/resources";
        String illinoisPath = basePath + "/svi_county_GISJOIN.Illinois.raw_data.json";

        this.illinoisSVI = spark.read()
                .option("multiLine", true)
                .json(illinoisPath);
        illinoisSVI = illinoisSVI.cache();

        System.out.println("Row count of Illinois SVI data: " + illinoisSVI.count());
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
        illinoisSVI.select(
                col("STATE"),
                col("COUNTY"),
                col("FIPS"),
                col("RPL_THEMES"),
                col("RPL_THEME1"),
                col("RPL_THEME3")).orderBy(col("RPL_THEMES").desc()).show(n, false);
    }

    /**
     * Sort by overall SVI percentile (RPL_THEMES) in ascending order.
     * 
     * @param n number of counties.
     */
    private void leastVulnerableCounties(int n) {
        System.out.println("\nTop " + n + " Least Vulnerable Counties (RPL_THEMES in descending order)");
        illinoisSVI.select(
                col("STATE"),
                col("COUNTY"),
                col("FIPS"),
                col("RPL_THEMES"),
                col("RPL_THEME1"),
                col("RPL_THEME3")).orderBy(col("RPL_THEMES").asc()).show(n, false);
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
        illinoisSVI.select(
                col("STATE"),
                col("COUNTY"),
                col("FIPS"),
                col("EP_GROUPQ"),
                col("RPL_THEMES")).orderBy(col("EP_GROUPQ").desc()).show(n, false);
    }

    /**
     * Sort by Socioeconomic Status (RPL_THEME1)
     * 
     * @param n number of counties.
     */
    private void highestSocioeconomicVulnerability(int n) {
        System.out.println("\nTop " + n + " Counties by Socioeconomic Vulnerability (RPL_THEME1 in descending order)");
        illinoisSVI.select(
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
        System.out.println(
                "\nTop " + n + " Counties by Minority/Language Vulnerability (RPL_THEME3 in descending order)");
        illinoisSVI.select(
                col("STATE"),
                col("COUNTY"),
                col("FIPS"),
                col("RPL_THEME3"),
                col("EP_MINRTY"), // minority %
                col("EP_LIMENG") // liimited english
        ).orderBy(col("RPL_THEME3").desc()).show(n, false);
    }

}
