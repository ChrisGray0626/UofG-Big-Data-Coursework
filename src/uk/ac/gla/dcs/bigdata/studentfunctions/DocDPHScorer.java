package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryDocScore;
import uk.ac.gla.dcs.bigdata.studentstructures.TermCount;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/20
 */
@Slf4j
public class DocDPHScorer implements FlatMapFunction<TermCount, QueryDocScore> {

    private final Broadcast<List<Query>> queryListBroadcast;
    private final Broadcast<Long> docTotalInCorpusBroadcast;
    private final Broadcast<Map<String, Long>> termNumMapInCorpusBroadcast;
    private final Broadcast<Long> termTotalInCorpusBroadcast;

    public DocDPHScorer(Broadcast<List<Query>> queryListBroadcast, Broadcast<Long> docTotalInCorpusBroadcast,
                        Broadcast<Map<String, Long>> termNumMapInCorpusBroadcast,
                        Broadcast<Long> termTotalInCorpusBroadcast) {
        this.queryListBroadcast = queryListBroadcast;
        this.docTotalInCorpusBroadcast = docTotalInCorpusBroadcast;
        this.termNumMapInCorpusBroadcast = termNumMapInCorpusBroadcast;
        this.termTotalInCorpusBroadcast = termTotalInCorpusBroadcast;
    }

    @Override
    public Iterator<QueryDocScore> call(TermCount termCountInDoc) throws Exception {
        List<QueryDocScore> queryDocScores = new ArrayList<>();
        // Term total in document
        long termTotalInDoc = termCountInDoc.getTermTotal();
        // log.info("NewsId: {} termTotalInDoc: {} termNumInDocMap: {}", termCountInDoc.getNewsArticle().getId(), termTotalInDoc,
        //          termCountInDoc.getQueryTermNumMap());
        Map<String, Long> termNumInDocMap = termCountInDoc.getQueryTermNumMap();
        // Term total in corpus
        long termTotalInCorpus = termTotalInCorpusBroadcast.value();
        Map<String, Long> termNumInCorpusMap = termNumMapInCorpusBroadcast.value();
        // Calculate the term average total number in document
        long docTotalInCorpus = docTotalInCorpusBroadcast.value();
        long termAvgTotalInDoc = termTotalInCorpus / docTotalInCorpus;
        // List<Query> queries = queryListBroadcast.value();
        // for (Query query : queries) {
            Query query = termCountInDoc.getQuery();
            List<String> queryTerms = query.getQueryTerms();
            double DPHScoreTotal = 0;
            int DPHSoreCount = 0;
            // Calculate the DPH score by averaging the DPH score of each query term
            for (String queryTerm : queryTerms) {
                // Skip the query term not in the document
                if (!termNumInDocMap.containsKey(queryTerm)) {
                    continue;
                }
                long termNumInDoc = termNumInDocMap.get(queryTerm);
                long termNumInCorpus = termNumInCorpusMap.getOrDefault(queryTerm, 0L);
                DPHScoreTotal += DPHScorer.getDPHScore((short) termNumInDoc, (int) termNumInCorpus, (int) termTotalInDoc,
                        termAvgTotalInDoc, docTotalInCorpus);
                DPHSoreCount++;
            }
            // if (DPHSoreCount == 0) {
            //     continue;
            // }
            double DPHScore = DPHScoreTotal / DPHSoreCount;
            queryDocScores.add(new QueryDocScore(query, termCountInDoc.getNewsArticle(), DPHScore));
        // }

        return queryDocScores.iterator();
    }
}
