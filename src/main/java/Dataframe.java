import org.apache.spark.sql.*;
import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import spark.Request;
import spark.Response;
import spark.Route;

import java.util.List;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.*;


import static org.apache.spark.sql.functions.*;
import static spark.Spark.*;





public class Dataframe {
    private static Map<String, Integer> sortByValue(Map<String, Integer> unsortMap) {


        List<Map.Entry<String, Integer>> list =
                new LinkedList<Map.Entry<String, Integer>>(unsortMap.entrySet());


        Collections.sort(list, new Comparator<Map.Entry<String, Integer>>() {
            public int compare(Map.Entry<String, Integer> o1,
                               Map.Entry<String, Integer> o2) {
                return (o2.getValue()).compareTo(o1.getValue());
            }
        });
        Map<String, Integer> sortedMap = new LinkedHashMap<String, Integer>();
        for (Map.Entry<String, Integer> entry : list) {
            sortedMap.put(entry.getKey(), entry.getValue());
        }

        return sortedMap;
    }
    public static Map<String, Integer> countFrequencies(ArrayList<String> list)
    {

        // hash set is created and elements of
        // arraylist are inserted into it

        Map<String, Integer> hm = new HashMap<String, Integer>();
        for (String s : list){
            Integer c = hm.get(s);
            hm.put(s, (c == null) ? 1 : c + 1);
    }
        hm=sortByValue(hm);
        return hm;
    }
    public static HashMap<String, Long> chart_data(List<Row> list)
    {

        // hash set is created and elements of
        // arraylist are inserted into it

        HashMap<String, Long> chart_values = new HashMap<String, Long>();
        for(Row s: list.subList(0, 20)){
            String key=s.getString(0);
            Long value=s.getLong(1);
            chart_values.put(key,value);

        }

        return chart_values;
    }

    public static void main(String[] args){
        //turn off info message in console
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        //initializing spark
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark Reading CSV")
                .config("spark.master", "local")
                .getOrCreate();
        //read csv file //turn first row to a header
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header","true")
                .load("E:\\S\\AI-PRO\\javaProject\\Project\\Wuzzuf_Jobs.csv");

        df.show(); //display first 20 rows of the dataframe
        //df.schema();
        df.describe(); //display data types of columns
        df.summary("count").show(); //shw data summary
        get("/hello", (req, res) -> "Hello World");

        Dataset<Row> df_clean=df.dropDuplicates(); //drop duplicated rows and save in new dataset df_clean
        df_clean=df_clean.na().drop(); //drop null data in place

        //group data by column name //count the repetition of each // sort in new col named count
        Dataset<Row> jobs_count=df_clean.groupBy("Company").count().sort(desc("count"));
        jobs_count.show();
        Dataset<Row> title_count=df_clean.groupBy("Title").count().sort(desc("count"));
        title_count.show();
        Dataset<Row> area_count=df_clean.groupBy("Location").count().sort(desc("count"));
        area_count.show();

        //changing data format to list to send to requests
        List<Row> jobs_data=jobs_count.collectAsList();
        List<Row> title_data=title_count.collectAsList();
        List<Row> area_data=area_count.collectAsList();
        //changing data format to hashmaps to send  to charting methods
        HashMap<String, Long> Jobs_chart=chart_data(jobs_data);
        HashMap<String, Long> titles_chart=chart_data(title_data);
        HashMap<String, Long> areas_chart=chart_data(area_data);


        //requests
        get("/jobs_count", new Route() {
            @Override
            public Object handle(Request request, Response response) {
                // process request
                return jobs_data.subList(0, 100);
            }
        });
        get("/title_count", new Route() {
            @Override
            public Object handle(Request request, Response response) {
                // process request
                return title_data.subList(0, 100);
            }
        });
        get("/area_count", new Route() {
            @Override
            public Object handle(Request request, Response response) {
                // process request
                return area_data.subList(0, 100);
            }
        });

        //display skills count one by one
        List<String> Skills= df_clean.select("Skills").as(Encoders.STRING()).collectAsList(); //collect skills col using select in a list of strings
        ArrayList<String> Details_Skills= new ArrayList<String>();
        for(int i=0; i<Skills.size();i++){
            Details_Skills.addAll(Arrays.asList(Skills.get(i).split(","))); //split list elements by "," and turning returned array to a list
        }

        for(int i=0;i<Details_Skills.size();i++) {
            Details_Skills.set(i, Details_Skills.get(i).trim().toLowerCase()); //clean data by deleting space and unifying them to lower case to avoid case sensitivity
        }

        //val allSkills = df_clean.select("Title", explode(split("Skills", ",")).as("word"));
        Map<String, Integer> Skills_Value=countFrequencies(Details_Skills); //count how many times a skill shows in a map
        for (Entry m : Skills_Value.entrySet())
            System.out.println("Frequency of " + m.getKey() + " is " + m.getValue()); //display frequency of required skills one by one
        //request for showing skill frequency
        get("/skills_count", new Route() {
            @Override
            public Object handle(Request request, Response response) {
                // process request
                return Skills_Value;
            }
        });
        //calling charts methods on results
        Pie_Chart.MakeChart(Jobs_chart);
        Bar_chart.MakeChart(titles_chart);
        Bar_chart.MakeChart(areas_chart);






    }






    }


