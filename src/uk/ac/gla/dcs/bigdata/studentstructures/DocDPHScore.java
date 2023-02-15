package uk.ac.gla.dcs.bigdata.studentstructures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DocDPHScore {

    private Query query;
    private NewsArticle newsArticle;
    private double DPHScore;
}
