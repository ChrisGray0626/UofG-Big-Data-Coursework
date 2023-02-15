package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.Map;
import java.util.Set;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryTerm;
import uk.ac.gla.dcs.bigdata.studentstructures.Term;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/19
 */
public class TermFilterByQueryTerm implements FilterFunction<Term> {
    private final Broadcast<Map<String, QueryTerm>> queryTermMapBroadcast;


    public TermFilterByQueryTerm(Broadcast<Map<String, QueryTerm>> queryTermMapBroadcast) {
        this.queryTermMapBroadcast = queryTermMapBroadcast;
    }

    @Override
    public boolean call(Term term) throws Exception {
        boolean isMatch = queryTermMapBroadcast.value().containsKey(term.getTerm());
        if (isMatch) {
            term.setQuery(queryTermMapBroadcast.value().get(term.getTerm()).getQuery());
            return true;
        } else {
            return false;
        }
    }
}
