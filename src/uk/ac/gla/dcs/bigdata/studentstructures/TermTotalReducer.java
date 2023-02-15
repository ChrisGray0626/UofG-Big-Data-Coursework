package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.Map;
import org.apache.spark.api.java.function.ReduceFunction;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/20
 */
public class TermTotalReducer implements ReduceFunction<NewsTermTotal> {

    @Override
    public NewsTermTotal call(NewsTermTotal v1, NewsTermTotal v2) throws Exception {
        // Reduce the term num map
        TermCount termCount1 = v1.getTermCount();
        Map<String, Long> termNumMap = termCount1.getQueryTermNumMap();
        TermCount termCount2 = v2.getTermCount();
        termCount2.getQueryTermNumMap().forEach(
                (term, num) -> termNumMap.put(term, termNumMap.getOrDefault(term, 0L) + num));
        termCount1.setQueryTermNumMap(termNumMap);
        termCount1.setTermTotal(termCount1.getTermTotal() + termCount2.getTermTotal());

        return v1;
    }
}
