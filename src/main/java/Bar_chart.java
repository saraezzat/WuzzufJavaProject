import javafx.scene.chart.CategoryAxis;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class Bar_chart {
    public static void MakeChart(HashMap<String, Long> map){
        Integer count=0; // initializing a count value to display a suitable number of results

        CategoryChart chart =new CategoryChartBuilder().width(1000).height(1000).title(Bar_chart.class.getSimpleName()).build(); //initializing  the chart builder

        chart.addSeries("Series1", new ArrayList<String>(map.keySet()),new ArrayList<Long>(map.values())); //take data from an array list as series to build
        chart.getStyler().setLegendVisible(false);
        chart.getStyler().setXAxisLabelRotation(90); // rotating strings to vertical  for a better display
        new SwingWrapper<CategoryChart>(chart).displayChart(); //draw and display graph

    }



}
