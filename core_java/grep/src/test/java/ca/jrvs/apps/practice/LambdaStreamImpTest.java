package ca.jrvs.apps.practice;

import ca.jrvs.apps.practice.*;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.*;
import static org.junit.Assert.*;

public class LambdaStreamImpTest {

    LambdaStreamExc lse = new LambdaStreamImp();

    @Test
    public void testToUpperCase() {
        List<String> result = lse.toUpperCase("a", "b", "c").collect(Collectors.toList());
        assertEquals(Arrays.asList("A","B","C"), result);
    }

    @Test
    public void testFilter() {
        Stream<String> input = Stream.of("apple","banana","car");
        List<String> result = lse.filter(input, "an").collect(Collectors.toList());
        assertEquals(Arrays.asList("banana"), result);
    }

    @Test
    public void testGetOdd() {
        List<Integer> result = lse.getOdd(IntStream.rangeClosed(0, 6)).boxed().collect(Collectors.toList());
        assertEquals(Arrays.asList(1,3,5), result);
    }

    @Test
    public void testFlatNestedInt() {
        Stream<List<Integer>> input = Stream.of(
                Arrays.asList(1,2),
                Arrays.asList(3),
                Arrays.asList(4,5)
        );
        assertEquals(Arrays.asList(1,2,3,4,5), lse.flatNestedInt(input).collect(Collectors.toList()));
    }
}

