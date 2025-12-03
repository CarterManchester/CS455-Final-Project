
package svi_prison_analysis;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.sql.*;

import svi_prison_analysis.ML.PrisonClassificationModel;
import svi_prison_analysis.ML.PrisonPCA;

import java.util.stream.Collectors;


// import static org.apache.spark.sql.functions.*;
// import static org.apache.spark.sql.functions.split;
import java.util.*;

// import svi_prison_analysis.States.*;

public class Main {

    public static final String[] ALL_THEME_VARS = {
        // --- Theme 1: Socioeconomic Status ---
        "EP_POV", 
        "EP_UNEMP", 
        "EP_PCI",
        "EP_NOHSDP",
        "EP_UNINSUR",

        // --- Theme 2: Household Characteristics ---
        "EP_DISABL", 
        "EP_SNGPNT", 
        "EP_LIMENG", 

        // --- Theme 3: Racial & Ethnic Minority Status & Language ---
        "EP_MINRTY",

        // --- Theme 4: Housing Type & Transportation ---
        "EP_MUNIT", 
        "EP_MOBILE", 
        "EP_CROWD", 
        "EP_NOVEH"
    };

    public static final String[] THEME_1_VARS = {
        "EP_POV", 
        "EP_UNEMP", 
        "EP_PCI", 
        "EP_NOHSDP", 
        "EP_UNINSUR"
    };

    public static final String[] THEME_2_VARS = {
        "EP_DISABL", 
        "EP_SNGPNT", 
        "EP_LIMENG"
    };

    public static final String[] THEME_4_VARS = {
        "EP_MUNIT", 
        "EP_MOBILE", 
        "EP_CROWD", 
        "EP_NOVEH", 
    };

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Investigating Correlations Between Social Vulnerability and Prison Populations")
                .master("local[*]") // IMPORTANT!! LOCAL MODE! Comment out (or change?) when testing on HDFS cluster!
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        spark.conf().set("spark.sql.debug.maxToStringFields", "200"); // NOTE: This is so it prints strings without size warnings. Change back if needed!!

        // NOTE: quick spark test to see if spark is running (print 0-10):
        // Dataset<Row> df = spark.range(0, 10).toDF("value");
        // df.show();


        // ========= Individual States =========
        State colorado = new State(spark, "Colorado");
        State illinois = new State(spark, "Illinois");
        colorado.runSVI();
        illinois.runSVI();


        // ========= Joined States =========
        Dataset<Row> combinedStates = buildCombinedStates(10, illinois, colorado);

        // ========= Main Model with All Themes 
        Dataset<Row> allThemes = sortByTheme(combinedStates, ALL_THEME_VARS);
        PrisonClassificationModel.trainAndEvaluate(allThemes);

        // ========= Run Model Based on SVI Themes
        Dataset<Row> theme1 = sortByTheme(combinedStates, THEME_1_VARS);
        PrisonClassificationModel.trainAndEvaluate(theme1);
        
        Dataset<Row> theme2 = sortByTheme(combinedStates, THEME_2_VARS);
        PrisonClassificationModel.trainAndEvaluate(theme2);
        
        Dataset<Row> theme4 = sortByTheme(combinedStates, THEME_4_VARS);
        PrisonClassificationModel.trainAndEvaluate(theme4);

        // ========= Run PCA Based on ALL Themes
        PrisonPCA.runPCA(combinedStates, ALL_THEME_VARS, 3);
        PrisonPCA.runPCA(combinedStates, new String[] { "EP_POV", "EP_UNEMP" }, 2);

        spark.stop();
    }

    /**
     * A method for joining states and data. Change or copy or add more as we need!
     * HOWEVER I dont think this is completely justified since its based on vastly
     * different population sizes right?
     * 
     * @param n      number of counties.
     * @param states each state included in the combined data of states.
     */
    private static Dataset<Row> buildCombinedStates(int n, State... states) {// no way this 'State... states'
                                                                                    // works bro... why didnt i know
                                                                                    // about this before??
        List<State> stateList = Arrays.asList(states);
        Dataset<Row> combinedData = null;

        // Dataset<Row> data = stateList.get(0).getJoinedCountyData();
        // String[] allColumnNames = data.columns();
        // System.out.println("=====================================================================");
        // System.out.println(Arrays.toString(allColumnNames));

        // List<String> filteredList = Arrays.stream(allColumnNames)
        //     .filter(s -> s.startsWith("E_") || s.startsWith("EP_"))
        //     .collect(Collectors.toList());
        
        // String[] filteredColumnNames = filteredList.toArray(new String[0]);

        for (State state : stateList) {
            Dataset<Row> temp = state.getJoinedCountyData().select("incarceration_rate", ALL_THEME_VARS);

            if (combinedData == null) {
                combinedData = temp;
            } else {
                combinedData = combinedData.unionByName(temp);
            }
        }

        System.out.println(
                "\n\n\n==============================================================================================");
        System.out.println(
                "\nTop " + n + " Most Vulnerable Counties Across All Provided States (incarceration_rate in descending order)");
        System.out.println(
                "==============================================================================================\n");

        combinedData.orderBy(col("incarceration_rate").desc()).show(n, false);
        return combinedData;
    }

    private static Dataset<Row> sortByTheme(Dataset<Row> data, String[] themeVars){
        return data.select("incarceration_rate", themeVars);
    }
}
