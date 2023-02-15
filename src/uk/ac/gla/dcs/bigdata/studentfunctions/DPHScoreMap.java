package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.NewsTermTotal;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryDocScore;
import uk.ac.gla.dcs.bigdata.studentstructures.TermCount;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/20
 */
@Slf4j
public class DPHScoreMap implements FlatMapFunction<NewsTermTotal, QueryDocScore> {

    private final Broadcast<List<Query>> queryListBroadcast;
    private final Broadcast<TermCount> termTotalInCorpusBroadcast;
    private final Broadcast<Long> docTotalInCorpusBroadcast;

    public DPHScoreMap(Broadcast<List<Query>> queryListBroadcast, Broadcast<TermCount> termTotalInCorpusBroadcast,
                       Broadcast<Long> docTotalInCorpusBroadcast) {
        this.queryListBroadcast = queryListBroadcast;
        this.termTotalInCorpusBroadcast = termTotalInCorpusBroadcast;
        this.docTotalInCorpusBroadcast = docTotalInCorpusBroadcast;
    }

    @Override
    public Iterator<QueryDocScore> call(NewsTermTotal newsTermTotal) throws Exception {
        // Term total in document
        TermCount termCount = newsTermTotal.getTermCount();
        long termTotalInDoc = termCount.getTermTotal();
        Map<String, Long> termNumInDocMap = termCount.getQueryTermNumMap();
        // Term total in corpus
        TermCount termCountInCorpusWrapper = termTotalInCorpusBroadcast.value();
        long termTotalInCorpus = termCountInCorpusWrapper.getTermTotal();
        Map<String, Long> termNumInCorpusMap = termCountInCorpusWrapper.getQueryTermNumMap();
        // Calculate the term average total number in document
        long docTotalInCorpus = docTotalInCorpusBroadcast.value();
        long termAvgTotalInDoc = termTotalInCorpus / docTotalInCorpus;
        List<Query> queries = queryListBroadcast.value();
        List<QueryDocScore> queryDocScores = new ArrayList<>();
        for (Query query : queries) {
            List<String> queryTerms = query.getQueryTerms();
            double DPHScoreTotal = 0;
            int DPHSoreCount = 0;
            // Calculate the DPH score by averaging the DPH score of each query term
            for (String queryTerm : queryTerms) {
                // Document does not contain any query term
                if (termNumInDocMap == null) {
                    continue;
                }
                // Document does not contain this query term
                if (!termNumInDocMap.containsKey(queryTerm)) {
                    continue;
                }
                long termNumInDoc = termNumInDocMap.get(queryTerm);
                long termNumInCorpus = termNumInCorpusMap.getOrDefault(queryTerm, 0L);
                DPHScoreTotal += DPHScorer.getDPHScore((short) termNumInDoc, (int) termNumInCorpus, (int) termTotalInDoc,
                        termAvgTotalInDoc, docTotalInCorpus);
                DPHSoreCount++;
            }
            if (DPHSoreCount == 0) {
                continue;
            }
            double DPHScore = DPHScoreTotal / DPHSoreCount;
            queryDocScores.add(new QueryDocScore(query, newsTermTotal.getNewsArticle(), DPHScore));
        }

        return queryDocScores.iterator();
    }
}
