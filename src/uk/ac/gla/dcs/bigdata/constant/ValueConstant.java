package uk.ac.gla.dcs.bigdata.constant;

/**
 * @Description Value Constant
 * @Author Xiaohui Yu
 * @Date 2023/2/16
 */
public class ValueConstant {

    public static final String SPARK_MASTER = "local[7]";
    public static final String EXECUTOR_CORE_NUM = "3";
    public static final String EXECUTOR_MEMORY = "1g";
    public static final String PARTITION_NUM = "48";
    public static final double SIMILARITY_THRESHOLD = 0.5;
    public static final int RANKING_SIZE = 10;
    public static final String SAMPLE_NEWS_FILE_PATH = "data/TREC_Washington_Post_collection.v3.example.json";
    public static final String FIX_NEWS_FILE_PATH = "data/TREC_Washington_Post_collection.v2.jl.fix.json";
    public static final String NEWS_FILE_PATH = FIX_NEWS_FILE_PATH;

}
