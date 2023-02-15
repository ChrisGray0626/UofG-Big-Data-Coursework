package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.ReduceFunction;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.DocScore;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryDocScore;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/21
 */
@Slf4j
public class QueryDocScoreReducer implements ReduceFunction<QueryDocScore> {
    @Override
    public QueryDocScore call(QueryDocScore v1, QueryDocScore v2) throws Exception {
        v2.getResults().forEach((query, docScores2) -> {
            List<DocScore> docScores1 = v1.getResults().getOrDefault(query, new ArrayList<>());
            docScores1.addAll(docScores2);
            v1.getResults().put(query, docScores1);
        });

        return v1;
    }
}
