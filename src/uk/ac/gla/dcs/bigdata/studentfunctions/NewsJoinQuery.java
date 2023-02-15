package uk.ac.gla.dcs.bigdata.studentfunctions;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.studentstructures.DocDPHScore;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/20
 */
public class NewsJoinQuery implements MapFunction<NewsArticle, DocDPHScore> {

    private LongAccumulator docTotalNumInCorpus;

    @Override
    public DocDPHScore call(NewsArticle value) throws Exception {
        return null;
    }
}
