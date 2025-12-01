package svi_prison_analysis;

import org.apache.spark.sql.*;
import static org.apache.spark.sql.functions.*;
// import static org.apache.spark.sql.functions.split;
// import java.util.*;


/**
 * This is the super class for any given US state. It finds the dataset JSON file, and then runs Spark operations
 * in order to sort data the way we need it to be sorted for every state.
 */
public class State {
    private final SparkSession spark;
    private Dataset<Row> sviData;
    private Dataset<Row> prisonData;
    private Dataset<Row> prisonByCounty;
    private String stateName;

    

    public State(SparkSession spark, String stateName) {
        this.spark = spark;
        this.stateName = stateName;
    }


    public void runSVI() { // main method for a given US State
        // String stateName = getStateName();
        System.out.println("\n\n\n==============================================================================================");
        System.out.println("| " + stateName + " SVI Data");
        System.out.println("==============================================================================================\n");

        loadData();

        getPrisonPopulationPerCounty();

        //NOTE: Commented this out so I had less printing, can undo as we need info!
        // mostVulnerableCounties(10);
        // leastVulnerableCounties(10);
        // highestGroupQuarters(10);
        // highestSocioeconomicVulnerability(10);
        // highestMinorityVulnerability(10);
    }

    protected void loadData() {
        String basePath = "src/main/resources";
        String pathToStateSVIData = basePath + "/svi_county_GISJOIN." + stateName + ".raw_data.json";
        String pathToStatePrisonData = basePath + "/prison_boundaries_geo." + stateName + ".raw_data.json";
        
        this.sviData = spark.read()
            .option("multiLine", true) 
            .json(pathToStateSVIData);
        sviData = sviData.cache();        
        
        this.prisonData = spark.read()
            .option("multiLine", true) 
            .json(pathToStatePrisonData);
        prisonData = prisonData.cache();      

        System.out.println("  - Row count of " + stateName + " SVI data: " + sviData.count());
        // System.out.println("==============================================================================================\n ");
        System.out.println("----------------------------------------------------------------------------------------------");
        System.out.println("  - Row count of " + stateName + " Prison data: " + prisonData.count());
        // System.out.println("==============================================================================================\n ");
        System.out.println("----------------------------------------------------------------------------------------------");
    }

    
    public Dataset<Row> getsviData(){
        return sviData;
    }






    // ==================== sorting data below! ====================

    // see README for variable meanings!!


    /**
     * Sort Prisons dataset to only include population and county
     * 
     * @return New dataset with only population and county
     */
    private Dataset<Row> getPrisonPopulationPerCounty(){
        Dataset<Row> popAndCounty = prisonData.select(
            col("properties.POPULATION").as("POPULATION"),
            col("properties.COUNTY").as("COUNTY"))
            .filter(col("POPULATION").notEqual(-999))
            .filter(col("POPULATION").notEqual(0))
            .groupBy("COUNTY")
            .agg(sum(col("POPULATION")).as("TOTAL_PRISON_POP"))
            .cache();

        popAndCounty.show();
        return popAndCounty;
    }

    /**
     * Joining SVI + TOTAL_PRISON_POP + incarceration_rate per county
     */
    public Dataset<Row> getJoinedCountyData(){
        Dataset<Row> svi = getsviData();
        Dataset<Row> prisons = getPrisonPopulationPerCounty();

        Dataset<Row> sviNorm = svi.withColumn( // sets prison and svi naming scheme to the same
            "countyAllCaps", 
            upper(col("COUNTY"))
        ); 
        Dataset<Row> prisonsNorm = prisons.withColumn(
            "countyAllCaps",
            upper(trim(col("COUNTY")))
        );

        System.out.println("\n=== Distinct SVI countyAllCaps (" + stateName + ") ===");
        sviNorm.select("COUNTY", "countyAllCaps").distinct().orderBy(col("countyAllCaps")).show(50, false);

        System.out.println("\n=== Distinct prison countyAllCaps (" + stateName + ") ===");
        prisonsNorm.select("COUNTY", "countyAllCaps").distinct().orderBy(col("countyAllCaps")).show(50, false);


        Dataset<Row> joined = sviNorm.join(prisonsNorm.select("countyAllCaps", "TOTAL_PRISON_POP"), "countyAllCaps");

        joined = joined.withColumn(
            "incarceration_rate", 
            col("TOTAL_PRISON_POP").cast("double").divide(col("E_TOTPOP").cast("double"))
        );

        joined = joined.filter(col("incarceration_rate").isNotNull());

        return joined;
    }

    /**
     * Sort by overall SVI percentile (RPL_THEMES) in descending order.
     * 
     * @param n number of counties.
     */
    private void mostVulnerableCounties(int n) {
        System.out.println("\nTop " + n + " Most Vulnerable Counties (RPL_THEMES in descending order)");
        sviData.select(
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
        sviData.select(
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
        sviData.select(
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
        sviData.select(
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
        sviData.select(
                col("STATE"),
                col("COUNTY"),
                col("FIPS"),
                col("RPL_THEME3"),
                col("EP_MINRTY"), // minority %
                col("EP_LIMENG") // liimited english
        ).orderBy(col("RPL_THEME3").desc()).show(n, false);
    }

}
