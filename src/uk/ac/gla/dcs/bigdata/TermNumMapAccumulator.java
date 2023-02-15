package uk.ac.gla.dcs.bigdata;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.spark.util.AccumulatorV2;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/21
 */
public class TermNumMapAccumulator extends AccumulatorV2<String, Map<String, Long>> {

    private Map<String, Long> termNumMap = new ConcurrentHashMap<>();


    @Override
    public boolean isZero() {
        return termNumMap.isEmpty();
    }

    @Override
    public AccumulatorV2<String, Map<String, Long>> copy() {
        TermNumMapAccumulator termNumMapAccumulator = new TermNumMapAccumulator();
        termNumMapAccumulator.termNumMap = new HashMap<>(termNumMap);
        return termNumMapAccumulator;
    }

    @Override
    public void reset() {
        termNumMap.clear();
    }

    @Override
    public void add(String v) {
        termNumMap.put(v, termNumMap.getOrDefault(v, 0L) + 1);
    }

    @Override
    public void merge(AccumulatorV2<String, Map<String, Long>> other) {
        other.value().forEach((k, v) -> termNumMap.merge(k, v, Long::sum));
    }

    @Override
    public Map<String, Long> value() {
        return termNumMap;
    }
}
