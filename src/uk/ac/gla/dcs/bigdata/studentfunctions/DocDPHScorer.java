package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryDocScore;
import uk.ac.gla.dcs.bigdata.studentstructures.TermCount;

/**
 * @Description
 * @Author Xiaohui Yu
 * @Date 2023/2/20
 */
@Slf4j
@AllArgsConstructor
public class DocDPHScorer implements FlatMapFunction<TermCount, QueryDocScore> {

    private final Broadcast<Long> docTotalInCorpusBroadcast;
    private final Broadcast<Map<String, Long>> termNumMapInCorpusBroadcast;
    private final Broadcast<Long> termTotalInCorpusBroadcast;

    @Override
    public Iterator<QueryDocScore> call(TermCount termCountInDoc) throws Exception {
        List<QueryDocScore> queryDocScores = new ArrayList<>();
        // Term count in document
        long termTotalInDoc = termCountInDoc.getTermTotal();
        Map<String, Long> termNumInDocMap = termCountInDoc.getQueryTermNumMap();
        // Term Count in corpus
        long termTotalInCorpus = termTotalInCorpusBroadcast.value();
        Map<String, Long> termNumInCorpusMap = termNumMapInCorpusBroadcast.value();
        // Calculate the term average total number in document
        long docTotalInCorpus = docTotalInCorpusBroadcast.value();
        long termAvgTotalInDoc = termTotalInCorpus / docTotalInCorpus;
        Query query = termCountInDoc.getQuery();
        List<String> queryTerms = query.getQueryTerms();
        double DPHScoreTotal = 0;
        int DPHScoreCount = 0;
        // Calculate the DPH score by averaging the DPH score of each query term
        for (String queryTerm : queryTerms) {
            // Calculate when the document includes this query term, because of log function, otherwise the score = 0
            if (termNumInDocMap.containsKey(queryTerm)) {
                long termNumInDoc = termNumInDocMap.get(queryTerm);
                long termNumInCorpus = termNumInCorpusMap.get(queryTerm);
                DPHScoreTotal += DPHScorer.getDPHScore((short) termNumInDoc, (int) termNumInCorpus, (int) termTotalInDoc,
                        termAvgTotalInDoc, docTotalInCorpus);
            }
            DPHScoreCount++;
        }
        double DPHScore = DPHScoreTotal / DPHScoreCount;

        queryDocScores.add(new QueryDocScore(query, termCountInDoc.getNewsArticle(), DPHScore));

        return queryDocScores.iterator();
    }
}
