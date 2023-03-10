package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/**
 * @Description Term count structure
 * @Author Xiaohui Yu
 * @Date 2023/2/20
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TermCount implements Serializable {

    private NewsArticle newsArticle;
    private Query query;
    private Map<String, Long> queryTermNumMap;
    private long termTotal;
}
