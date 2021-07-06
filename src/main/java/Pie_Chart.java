import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;

import java.util.HashMap;
import java.util.Map;

public class Pie_Chart {
    public static void MakeChart(HashMap<String, Long> map){
        Integer count=0; //initializing counter to display suitable number of results

        PieChart chart =new PieChartBuilder().width(800).height(600).title(Pie_Chart.class.getSimpleName()).build(); //initializing chart with suitable size

        for (Map.Entry<String, Long> entry : map.entrySet()) {
            chart.addSeries(entry.getKey(), entry.getValue()); //send company's details from a map as series
            count++;
            if (count == 20) {
                break; //show only 20 companies
            }
        }

        new SwingWrapper<PieChart>(chart).displayChart(); //draw and display the chart
    }



}
