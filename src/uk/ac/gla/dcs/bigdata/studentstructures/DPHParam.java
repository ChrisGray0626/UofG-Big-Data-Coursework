package uk.ac.gla.dcs.bigdata.studentstructures;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description Query term in corpus structure
 * @Author Xiaohui Yu
 * @Date 2023/2/15
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DPHParam {

    private String newsArticleId;
    private String term;
    // The number of terms in the document
    private long termNumInDoc;
    // The number of document in the corpus
    private long termNumInCorpus;
    // The total number of terms in the document
    private long termTotalInDoc;
}
