package uk.ac.gla.dcs.bigdata.studentstructures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description
 * @Author Xiaohui Yu
 * @Date 2023/2/19
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DocTermDPHScoreAndOriginalQuery {

    private String newsArticleId;
    private String originalQuery;
    private String term;
    private double score;
}
