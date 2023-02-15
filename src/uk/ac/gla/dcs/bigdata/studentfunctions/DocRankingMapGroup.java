package uk.ac.gla.dcs.bigdata.studentfunctions;

import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import org.apache.spark.api.java.function.MapGroupsFunction;
import uk.ac.gla.dcs.bigdata.constant.ValueConstant;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentstructures.DocDPHScore1;

/**
 * @Description Rank the documents based on the DPH score
 * @Author Xiaohui Yu
 * @Date 2023/2/16
 */
public class DocRankingMapGroup implements MapGroupsFunction<String, DocDPHScore1, DocumentRanking> {

    @Override
    public DocumentRanking call(String originalQuery, Iterator<DocDPHScore1> docDPHScoreIterator) throws Exception {
        Deque<RankedResult> rankedResults = new LinkedList<>();
        List<RankedResult> docDPHScoreList = new ArrayList<>();
        Query query = null;
        while (docDPHScoreIterator.hasNext()) {
            DocDPHScore1 nextItem = docDPHScoreIterator.next();
            query = nextItem.getQuery();
            RankedResult rankedResult = nextItem.getRankedResult();
            // Add the first item
            if (rankedResults.isEmpty()) {
                rankedResults.offer(rankedResult);
            }
            // Add the item if the similarity is less than the threshold
            else {
                RankedResult curItem = rankedResults.peek();
                double similarity =
                        TextDistanceCalculator.similarity(curItem.getArticle().getTitle(), nextItem.getTitle());
                if (similarity > ValueConstant.SIMILARITY_THRESHOLD) {
                    rankedResults.add(rankedResult);
                }
            }
            // Rank the first 10 items
            if (rankedResults.size() >= ValueConstant.RANKING_SIZE) {
                break;
            }
        }
        // Convert the Deque to List
        while (!rankedResults.isEmpty()) {
            docDPHScoreList.add(rankedResults.pollFirst());
        }

        return new DocumentRanking(query, docDPHScoreList);
    }
}
