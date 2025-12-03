
package svi_prison_analysis;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.regexp_replace;
import static org.apache.spark.sql.functions.trim;
import static org.apache.spark.sql.functions.upper;

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
        "EP_POV",       // % of people below 150% poverty estimate
        "EP_UNEMP",     // % of people of age of 16+ unemployed
        "EP_PCI",       // Per capita income (as % rank)
        "EP_NOHSDP",    // % of people of age 25+ with no high school diploma
        "EP_UNINSUR",   // % of people without health insurance

        // --- Theme 2: Household Characteristics ---
        "EP_DISABL",    // % of people with disabilities (not institutionalized)
        "EP_SNGPNT",    // % single parent households with children under 18
        "EP_LIMENG",    // % of people who speak English "less than well"

        // --- Theme 3: Racial & Ethnic Minority Status & Language ---
        "EP_MINRTY",    // % of people that is racial/ethnic minority

        // --- Theme 4: Housing Type & Transportation ---
        "EP_MUNIT",     // % of housing in structures with 10+ units (like apartments)
        "EP_MOBILE",    // % of housing that are mobile homes
        "EP_CROWD",     // % of households with more than 1 person per room (crowding)
        "EP_NOVEH"      // % of households with no available vehicle
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

    public static final String[] ALL_STATE_NAMES = {
        "Alabama", "Alaska", "Arizona", "Arkansas", "California",
        "Colorado", "Connecticut", "Delaware", "Florida", "Georgia",
        "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa",
        "Kansas", "Kentucky", "Louisiana", "Maine", "Maryland",
        "Massachusetts", "Michigan", "Minnesota", "Mississippi", "Missouri",
        "Montana", "Nebraska", "Nevada", "New Hampshire", "New Jersey",
        "New Mexico", "New York", "North Carolina", "North Dakota", "Ohio",
        "Oklahoma", "Oregon", "Pennsylvania", "Rhode Island", "South Carolina",
        "South Dakota", "Tennessee", "Texas", "Utah", "Vermont",
        "Virginia", "Washington", "West Virginia", "Wisconsin", "Wyoming"
    };

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("Investigating Correlations Between Social Vulnerability and Prison Populations")
                .master("local[*]") // IMPORTANT!! LOCAL MODE! Comment out (or change?) when testing on HDFS cluster!
                .getOrCreate();

        // spark.sparkContext().setLogLevel("WARN");
        spark.sparkContext().setLogLevel("ERROR");
        spark.conf().set("spark.sql.debug.maxToStringFields", "200"); // NOTE: This is so it prints strings without size warnings. Change back if needed!!


        List<State> states = new ArrayList<>();

        for(String s : ALL_STATE_NAMES){
            State temp = new State(spark, s);
            temp.runSVI();
            states.add(temp);
        }
        // ========= Individual States =========
        // State colorado = new State(spark, "Colorado");
        // State illinois = new State(spark, "Illinois");
        // colorado.runSVI();
        // illinois.runSVI();


        // ========= Joined States =========
        Dataset<Row> combinedStates = buildCombinedStates(10, states);
        Dataset<Row> mostVulnerable = mostVulnerableCounties(10, states);
        Dataset<Row> leaseVulnerable = leaseVulnerableCounties(10, states);
        Dataset<Row> focoBocoWy = getSelectedCounties(states);

        // ========= Main Model with All Themes 
        Dataset<Row> allThemes = sortByTheme(combinedStates, ALL_THEME_VARS);
        System.out.println("Running Gradient-Boosted Decision Tree with ALL Themes...");
        PrisonClassificationModel.trainAndEvaluate(allThemes);

        // ========= Run Model Based on SVI Themes
        Dataset<Row> theme1 = sortByTheme(combinedStates, THEME_1_VARS);
        System.out.println("\nRunning Gradient-Boosted Decision Tree with Socioeconomic Variables...");
        PrisonClassificationModel.trainAndEvaluate(theme1);
        
        Dataset<Row> theme2 = sortByTheme(combinedStates, THEME_2_VARS);
        System.out.println("\nRunning Gradient-Boosted Decision Tree with Household Characteristics Variables...");
        PrisonClassificationModel.trainAndEvaluate(theme2);
        
        Dataset<Row> theme4 = sortByTheme(combinedStates, THEME_4_VARS);
        System.out.println("\nRunning Gradient-Boosted Decision Tree with Housing Type and Transportation Access Variables...");
        PrisonClassificationModel.trainAndEvaluate(theme4);

        // ========= Run PCA Based on ALL Themes
        // System.out.println("/nRunning PCA with ... Themes...");
        // PrisonPCA.runPCA(combinedStates, ALL_THEME_VARS, 3);
        // PrisonPCA.runPCA(combinedStates, new String[] { "EP_POV", "EP_UNEMP" }, 2);

        spark.stop();
    }

    /**
     * A method for joining states and data. Change or copy or add more as we need!
     * 
     * @param n      number of counties.
     * @param states each state included in the combined data of states.
     */
    private static Dataset<Row> buildCombinedStates(int n, List<State> states) {
        Dataset<Row> combinedData = null;

        // Dataset<Row> data = stateList.get(0).getJoinedCountyData();
        // String[] allColumnNames = data.columns();
        // System.out.println("=====================================================================");
        // System.out.println(Arrays.toString(allColumnNames));

        // List<String> filteredList = Arrays.stream(allColumnNames)
        //     .filter(s -> s.startsWith("E_") || s.startsWith("EP_"))
        //     .collect(Collectors.toList());
        
        // String[] filteredColumnNames = filteredList.toArray(new String[0]);

        for (State state : states) {
            Dataset<Row> temp = state.getJoinedCountyData().select("incarceration_rate", ALL_THEME_VARS);

            if (combinedData == null) {
                combinedData = temp;
            } else {
                combinedData = combinedData.unionByName(temp);
            }
        }

        System.out.println("\n\n\n==============================================================================================");
        System.out.println("\nTop " + n + " Most Vulnerable Counties Across All Provided States (incarceration_rate in descending order)");
        System.out.print("==============================================================================================\n");

        combinedData.orderBy(col("incarceration_rate").desc()).show(n, false);
        return combinedData;
    }


    private static Dataset<Row> sortByTheme(Dataset<Row> data, String[] themeVars){
        return data.select("incarceration_rate", themeVars);
    }


    /**
     * Gets all of the most vulnerable counties in the list of states given.
     * 
     * @param n      number of counties.
     * @param states each state included in the combined data of states.
     */
    private static Dataset<Row> mostVulnerableCounties(int n, List<State> states) {
        Dataset<Row> combined = null;

        for (State state : states) {
            Dataset<Row> temp = state.getJoinedCountyData()
                    .select(
                            col("STATE"),
                            col("COUNTY"),
                            col("FIPS"),
                            col("E_TOTPOP"),
                            col("TOTAL_PRISON_POP"),
                            col("incarceration_rate"));

            if (combined == null) {
                combined = temp;
            } else {
                combined = combined.unionByName(temp);
            }
        }

        // Sort by incarceration_rate descending and keep only the top n
        Dataset<Row> topN = combined.orderBy(col("incarceration_rate").desc()).limit(n);

        System.out.println("\n\n\n==============================================================================================");
        System.out.println("Top " + n + " Counties by Incarceration Rate Across All States");
        System.out.print("==============================================================================================\n");
        topN.show(false);

        return topN;
    }



    /**
     * Gets all of the least vulnerable counties in the list of states given.
     * 
     * @param n      number of counties.
     * @param states each state included in the combined data of states.
     */
    private static Dataset<Row> leaseVulnerableCounties(int n, List<State> states) {
        Dataset<Row> combined = null;

        for (State state : states) {
            Dataset<Row> temp = state.getJoinedCountyData()
                .select(
                    col("STATE"),
                    col("COUNTY"),
                    col("FIPS"),
                    col("E_TOTPOP"),
                    col("TOTAL_PRISON_POP"),
                    col("incarceration_rate"));

            if (combined == null) {
                combined = temp;
            } else {
                combined = combined.unionByName(temp);
            }
        }

        Dataset<Row> bottomN = combined.orderBy(col("incarceration_rate").asc()).limit(n);

        System.out.println("\n\n\n==============================================================================================");
        System.out.println("Bottom " + n + " Counties by Incarceration Rate Across All States");
        System.out.print("==============================================================================================\n");
        bottomN.show(false);

        return bottomN;
    }


    /**
     * Returns a dataset containing ONLY Larimer County (CO), Boulder County (CO), Laramie County (WY)
     */
    private static Dataset<Row> getSelectedCounties(List<State> states) {
        Dataset<Row> combined = null;

        for (State s : states) {
            Dataset<Row> temp = s.getJoinedCountyData()
                .select(
                    col("STATE"),
                    col("COUNTY"),
                    col("FIPS"),
                    col("E_TOTPOP"),
                    col("TOTAL_PRISON_POP"),
                    col("incarceration_rate"));

            if (combined == null){
                combined = temp;
            }
            else{
                combined = combined.unionByName(temp);
            }
        }

        Dataset<Row> normalized = combined
            .withColumn("county_upper",upper(regexp_replace(trim(col("COUNTY")), " COUNTY$", "")))
            .withColumn("state_upper", upper(trim(col("STATE"))));

        Dataset<Row> selectedCounties = normalized.filter(
            col("county_upper").equalTo("LARIMER").and(col("state_upper").equalTo("COLORADO"))
            .or(col("county_upper").equalTo("BOULDER").and(col("state_upper").equalTo("COLORADO")))
            .or(col("county_upper").equalTo("LARAMIE").and(col("state_upper").equalTo("WYOMING")))
        );

        selectedCounties = selectedCounties.drop("county_upper", "state_upper");

        System.out.println("\n\n\n==============================================================================================");
        System.out.println("Larimer County (CO), Boulder County (CO), and Laramie County (WY)");
        System.out.print("==============================================================================================\n");
        selectedCounties.orderBy(col("incarceration_rate").desc()).show(false);

        return selectedCounties;
    }
}
