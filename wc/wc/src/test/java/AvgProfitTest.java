import org.junit.Test;

import com.thanhtt.flink.AvgProfit;

import static org.junit.Assert.*;

import org.apache.flink.api.java.tuple.Tuple5;

public class AvgProfitTest {

    @Test
    public void reduce() {
        Tuple5<String, String, String, Integer, Integer> current = new Tuple5<>("a", "b", "c", 1, 1);
        Tuple5<String, String, String, Integer, Integer> pre_result = new Tuple5<>("a", "b", "c", 2, 2);
        Tuple5<String, String, String, Integer, Integer> result = new Tuple5<>("a", "b", "c", 3, 3);

        AvgProfit.Reduce1 reduce = new AvgProfit.Reduce1();
        assertEquals(result, reduce.reduce(current, pre_result));
    }
}