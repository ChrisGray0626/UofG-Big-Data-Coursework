package uk.ac.gla.dcs.bigdata.providedstructures;

import java.io.Serializable;

public class RankedResult implements Serializable, Comparable<RankedResult> {

	private static final long serialVersionUID = -2905684103776472843L;
	
	String docId;
	NewsArticle article;
	double score;
	
	public RankedResult() {}
	
	public RankedResult(String docId, NewsArticle article, double score) {
		super();
		this.docId = docId;
		this.article = article;
		this.score = score;
	}

	public String getDocId() {
		return docId;
	}

	public void setDocId(String docId) {
		this.docId = docId;
	}

	public NewsArticle getArticle() {
		return article;
	}

	public void setArticle(NewsArticle article) {
		this.article = article;
	}

	public double getScore() {
		return score;
	}

	public void setScore(double score) {
		this.score = score;
	}

	@Override
	public int compareTo(RankedResult o) {
		return new Double(score).compareTo(o.score);
	}
	
	
	
}
