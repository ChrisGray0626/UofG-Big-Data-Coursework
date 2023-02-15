package uk.ac.gla.dcs.bigdata.studentstructures;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Description Document ranking structure
 * @Author Xiaohui Yu
 * @Date 2023/2/16
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class DocRanking {

    private String originalQuery;
    private List<DocDPHScore1> docDPHScore1s;
}
