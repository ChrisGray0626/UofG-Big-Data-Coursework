package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.List;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.studentstructures.DocDPHScore1;
import uk.ac.gla.dcs.bigdata.studentstructures.DocDPHScoreWithoutQuery;

/**
 * @Description
 * @Author Xiaohui Yu
 * @Date 2023/2/19
 */
public class DocDPHScoreJoinQuery implements MapFunction<DocDPHScoreWithoutQuery, DocDPHScore1> {

    private final List<Query> queries;

    public DocDPHScoreJoinQuery(Broadcast<List<Query>> broadcast) {
        this.queries = broadcast.value();
    }

    @Override
    public DocDPHScore1 call(DocDPHScoreWithoutQuery docDPHScoreWithoutQuery) throws Exception {
        String originalQuery = docDPHScoreWithoutQuery.getOriginalQuery();
        Query query = queries.stream().filter(item -> item.getOriginalQuery().equals(originalQuery)).findFirst().get();

        return new DocDPHScore1(docDPHScoreWithoutQuery.getOriginalQuery(), docDPHScoreWithoutQuery.getNewsArticle(), originalQuery, query, docDPHScoreWithoutQuery.getDPHScore());
    }
}
