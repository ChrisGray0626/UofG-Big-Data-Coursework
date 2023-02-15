package uk.ac.gla.dcs.bigdata.studentstructures;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;

/**
 * @Description Query term structure
 * @Author Xiaohui Yu
 * @Date 2023/2/15
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class QueryTerm implements Serializable {

    private Query query;
    private String term;

}
