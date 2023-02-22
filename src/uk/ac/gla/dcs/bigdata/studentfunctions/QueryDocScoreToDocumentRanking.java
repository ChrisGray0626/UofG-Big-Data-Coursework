package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.collections4.IteratorUtils;
import org.apache.spark.api.java.function.MapGroupsFunction;
import uk.ac.gla.dcs.bigdata.constant.ValueConstant;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryDocScore;

/**
 * @Description
 * @Author Xiaohui Yu
 * @Date 2023/2/20
 */
public class QueryDocScoreToDocumentRanking implements MapGroupsFunction<String, QueryDocScore, DocumentRanking> {

    @Override
    public DocumentRanking call(String key, Iterator<QueryDocScore> iterator) throws Exception {
        List<QueryDocScore> scores = IteratorUtils.toList(iterator);
        // Order the scores by the DPH score in descending order
        scores.sort(Comparator.comparing(QueryDocScore::getDPHScore).reversed());
        List<RankedResult> rankedResults = new ArrayList<>();
        Query query = scores.get(0).getQuery();
        for (QueryDocScore score : scores) {
            // Add the item if the similarity is less than the threshold
            boolean isSimilar = false;
            for (RankedResult rankedResult : rankedResults) {
                double similarity =
                        TextDistanceCalculator.similarity(rankedResult.getArticle().getTitle(), score.getNewsArticle().getTitle());
                if (similarity < ValueConstant.SIMILARITY_THRESHOLD) {
                    isSimilar = true;
                }
            }
            if (isSimilar) {
                continue;
            }
            rankedResults.add(new RankedResult(score.getNewsArticle().getId(), score.getNewsArticle(), score.getDPHScore()));
            // Rank the first 10 items
            if (rankedResults.size() >= ValueConstant.RANKING_SIZE) {
                break;
            }
        }

        return new DocumentRanking(query, rankedResults);
    }
}
