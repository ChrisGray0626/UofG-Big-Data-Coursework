package uk.ac.gla.dcs.bigdata.studentstructures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description Document term DPH score structure
 * @Author Xiaohui Yu
 * @Date 2023/2/15
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DocTermDPHScore {

    private String newsArticleId;
    private String term;
    private double score;
}
