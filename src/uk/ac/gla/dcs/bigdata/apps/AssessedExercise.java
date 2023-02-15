package uk.ac.gla.dcs.bigdata.apps;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.LongAccumulator;
import uk.ac.gla.dcs.bigdata.TermNumMapAccumulator;
import uk.ac.gla.dcs.bigdata.constant.ValueConstant;
import uk.ac.gla.dcs.bigdata.providedfunctions.NewsFormatter;
import uk.ac.gla.dcs.bigdata.providedfunctions.QueryFormaterMap;
import uk.ac.gla.dcs.bigdata.providedstructures.DocumentRanking;
import uk.ac.gla.dcs.bigdata.providedstructures.NewsArticle;
import uk.ac.gla.dcs.bigdata.providedstructures.Query;
import uk.ac.gla.dcs.bigdata.providedstructures.RankedResult;
import uk.ac.gla.dcs.bigdata.providedutilities.TextDistanceCalculator;
import uk.ac.gla.dcs.bigdata.studentfunctions.DocDPHScorer;
import uk.ac.gla.dcs.bigdata.studentfunctions.NewsToTermCountInDoc;
import uk.ac.gla.dcs.bigdata.studentstructures.DocScore;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryDocScore;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryDocScoreToDocumentRanking;
import uk.ac.gla.dcs.bigdata.studentstructures.QueryTerm;
import uk.ac.gla.dcs.bigdata.studentstructures.TermCount;

/**
 * This is the main class where your Spark topology should be specified.
 * <p>
 * By default, running this class will execute the topology defined in the
 * rankDocuments() method in local mode, although this may be overriden by
 * the spark.master environment variable.
 *
 * @author Richard
 */
@Slf4j
public class AssessedExercise {

    private static final String SAMPLE_NEWS_FILE_PATH = "data/TREC_Washington_Post_collection.v3.example.json";
    private static final String FIX_NEWS_FILE_PATH = "data/TREC_Washington_Post_collection.v2.jl.fix.json";
    private static final String NEWS_FILE_PATH = FIX_NEWS_FILE_PATH;
    private static final String EXECUTOR_CORE_NUM = "3";
    private static final String EXECUTOR_MEMORY = "1g";
    private static final String PARTITION_NUM = "48";


    public static void main(String[] args) {

        File hadoopDIR = new File(
                "resources/hadoop/"); // represent the hadoop directory as a Java file so we can get an absolute path for it
        System.setProperty("hadoop.home.dir",
                hadoopDIR.getAbsolutePath()); // set the JVM system property so that Spark finds it

        // The code submitted for the assessed exerise may be run in either local or remote modes
        // Configuration of this will be performed based on an environment variable
        String sparkSessionName = "BigDataAE"; // give the session a name

        // Create the Spark Configuration
        SparkConf conf = new SparkConf()
                .setMaster("local[16]")
                .set("spark.executor.memory", EXECUTOR_MEMORY)
                .set("spark.executor.cores", EXECUTOR_CORE_NUM)
                .setAppName(sparkSessionName);
        // Create the spark session
        SparkSession spark = SparkSession.builder().config(conf).getOrCreate();


        // Get the location of the input queries
        String queryFile = System.getenv("bigdata.queries");
        if (queryFile == null) {
            queryFile = "data/queries.list"; // default is a sample with 3 queries
        }

        // Get the location of the input news articles
        String newsFile = System.getenv("bigdata.news");
        if (newsFile == null) {
            newsFile = NEWS_FILE_PATH; // default is a sample of 5000 news articles
        }

        // Call the student's code
        List<DocumentRanking> results = rankDocuments(spark, queryFile, newsFile);

        // Close the spark session
        spark.close();

        // Check if the code returned any results
        if (results == null) {
            System.err.println(
                    "Topology return no rankings, student code may not be implemented, skiping final write.");
        } else {

            // We have set of output rankings, lets write to disk

            // Create a new folder
            File outDirectory = new File("results/" + System.currentTimeMillis());
            if (!outDirectory.exists()) {
                outDirectory.mkdir();
            }

            // Write the ranking for each query as a new file
            for (DocumentRanking rankingForQuery : results) {
                rankingForQuery.write(outDirectory.getAbsolutePath());
            }
        }


    }

    public static List<DocumentRanking> rankDocuments(SparkSession spark, String queryFile, String newsFile) {

        // Load queries and news articles
        Dataset<Row> queriesjson = spark.read().text(queryFile);
        Dataset<Row> newsjson = spark.read().text(newsFile); // read in files as string rows, one row per article
        // newsjson = newsjson.limit(40000);
        newsjson = newsjson.repartition(Integer.parseInt(PARTITION_NUM));
        // Perform an initial conversion from Dataset<Row> to Query and NewsArticle Java objects
        Dataset<Query> queries = queriesjson.map(new QueryFormaterMap(),
                Encoders.bean(Query.class)); // this converts each row into a Query
        // Convert queries to query terms
        Dataset<QueryTerm> queryTerms = queries.flatMap((FlatMapFunction<Query, QueryTerm>) query -> {
            List<QueryTerm> terms = new ArrayList<>();
            query.getQueryTerms().forEach(queryTerm -> terms.add(new QueryTerm(query, queryTerm)));
            return terms.iterator();
        }, Encoders.bean(QueryTerm.class));
        // Convert query terms to query term map
        Map<String, Query> queryTermMap =
                queryTerms.collectAsList().stream().collect(Collectors.toMap(QueryTerm::getTerm, QueryTerm::getQuery));
        Broadcast<Map<String, Query>> queryTermMapBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryTermMap);
        Set<String> queryTermSet = new HashSet<>(queryTermMap.keySet());
        // Broadcast the query term set
        Broadcast<Set<String>> queryTermSetBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryTermSet);
        LongAccumulator docTotalInCorpusAccumulator = spark.sparkContext().longAccumulator();
        TermNumMapAccumulator termNumMapInCorpusAccumulator = new TermNumMapAccumulator();
        LongAccumulator termTotalInCorpusAccumulator = spark.sparkContext().longAccumulator();
        spark.sparkContext().register(termNumMapInCorpusAccumulator, "termNumMapInCorpusAccumulator");
        // Convert news to term count in doc
        // Dataset<TermCount> termCountInDoc = newsjson.flatMap(
        //         new NewsJsonToTermCountInDoc(queryTermSetBroadcast, docTotalInCorpusAccumulator,
        //                 termNumMapInCorpusAccumulator, termTotalInCorpusAccumulator), Encoders.bean(TermCount.class));
        Dataset<NewsArticle> news = newsjson.flatMap(new NewsFormatter(),
                Encoders.bean(NewsArticle.class)); // this converts each row into a NewsArticle

        // JavaRDD<NewsArticle> newsArticleJavaRDD = news.toJavaRDD();
        // JavaPairRDD<String, NewsArticle> newsArticleJavaPairRDD = newsArticleJavaRDD.mapToPair(
        //         (PairFunction<NewsArticle, String, NewsArticle>) newsArticle -> new Tuple2<>(newsArticle.getId(),
        //                 newsArticle));
        // JavaPairRDD<String, NewsArticle> newsJavaPairRDDPartitioned =
        //         newsArticleJavaPairRDD.partitionBy(new StringPartitioner(PARTITION_NUM));
        // JavaRDD<NewsArticle> newsJavaRDDPartitioned = newsJavaPairRDDPartitioned.map(x -> x._2);
        // news = spark.createDataset(newsJavaRDDPartitioned.rdd(), Encoders.bean(NewsArticle.class));


        // Calculate the document total number in the corpus

        Dataset<TermCount> termCountInDoc = news.flatMap(new NewsToTermCountInDoc(queryTermMapBroadcast, termNumMapInCorpusAccumulator, docTotalInCorpusAccumulator, termTotalInCorpusAccumulator), Encoders.bean(TermCount.class));
        log.info("termCountInDoc count: " + termCountInDoc.cache().count());
        List<Query> queryList = queries.collectAsList();
        Broadcast<List<Query>> queryListBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(queryList);
        long docTotalInCorpus = docTotalInCorpusAccumulator.value();
        Broadcast<Long> docTotalInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(docTotalInCorpus);
        Map<String, Long> termNumMapInCorpus = termNumMapInCorpusAccumulator.value();
        Broadcast<Map<String, Long>> termNumMapInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termNumMapInCorpus);
        long termTotalInCorpus = termTotalInCorpusAccumulator.value();
        Broadcast<Long> termTotalInCorpusBroadcast = JavaSparkContext.fromSparkContext(spark.sparkContext()).broadcast(termTotalInCorpus);
        Dataset<QueryDocScore> queryDocScores = termCountInDoc.flatMap(
                new DocDPHScorer(queryListBroadcast, docTotalInCorpusBroadcast, termNumMapInCorpusBroadcast, termTotalInCorpusBroadcast), Encoders.bean(
                        QueryDocScore.class));
        // QueryDocScore queryDocScore = queryDocScores.reduce(new QueryDocScoreReducer());
        // List<DocumentRanking> documentRankings = queryDocScoreToDocumentRanking(queryDocScore);
        List<DocumentRanking> documentRankings = queryDocScores.groupByKey(
                        (MapFunction<QueryDocScore, String>) (score) -> score.getQuery().getOriginalQuery(),
                        Encoders.STRING()).mapGroups(new QueryDocScoreToDocumentRanking(), Encoders.bean(DocumentRanking.class))
                .collectAsList();
        return documentRankings;
    }

    public static List<DocumentRanking> queryDocScoreToDocumentRanking(QueryDocScore queryDocScore) {
        List<DocumentRanking> documentRankings = new ArrayList<>();
        queryDocScore.getResults().forEach((query, docScores) -> {
            docScores.sort(Comparator.comparing(DocScore::getScore).reversed());
            List<RankedResult> rankedResults = new ArrayList<>();
            for (DocScore docScore : docScores) {
                // Add the item if the similarity is less than the threshold
                boolean isSimilar = false;
                for (RankedResult rankedResult : rankedResults) {
                    double similarity = TextDistanceCalculator.similarity(rankedResult.getArticle().getTitle(),
                            docScore.getNewsArticle().getTitle());
                    if (similarity < ValueConstant.SIMILARITY_THRESHOLD) {
                        isSimilar = true;
                    }
                }
                if (isSimilar) {
                    continue;
                }
                rankedResults.add(
                        new RankedResult(docScore.getNewsArticle().getId(), docScore.getNewsArticle(), docScore.getScore()));
                // Rank the first 10 items
                if (rankedResults.size() >= ValueConstant.RANKING_SIZE) {
                    break;
                }
            }
            documentRankings.add(new DocumentRanking(query, rankedResults));
        });
        return documentRankings;
    }
}
