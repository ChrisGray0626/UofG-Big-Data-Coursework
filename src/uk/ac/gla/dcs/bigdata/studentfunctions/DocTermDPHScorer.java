package uk.ac.gla.dcs.bigdata.studentfunctions;

import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.function.MapFunction;
import uk.ac.gla.dcs.bigdata.providedutilities.DPHScorer;
import uk.ac.gla.dcs.bigdata.studentstructures.DocTermDPHScore;
import uk.ac.gla.dcs.bigdata.studentstructures.Term;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/20
 */
@Slf4j
public class DocTermDPHScorer implements MapFunction<Term, Term> {

    private final double termAvgTotalInDoc;
    private final long docNumInCorpus;

    public DocTermDPHScorer(double termAvgTotalInDoc, long docNumInCorpus) {
        this.termAvgTotalInDoc = termAvgTotalInDoc;
        this.docNumInCorpus = docNumInCorpus;
    }

    @Override
    public Term call(Term term) throws Exception {
        long termNumInDoc = term.getTermNumInDoc();
        long termNumInCorpus = term.getTermNumInCorpus();
        long termTotalInDoc = term.getTermTotalInDoc();
        // Calculate the DPH score
        double DPHScore = DPHScorer.getDPHScore((short) termNumInDoc, (int) termNumInCorpus, (int) termTotalInDoc,
                termAvgTotalInDoc, docNumInCorpus);
        term.setDPHScore(DPHScore);

        return term;
    }
}
