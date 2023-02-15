package uk.ac.gla.dcs.bigdata;

import org.apache.spark.Partitioner;

/**
 * @Description
 * @Author Chris
 * @Date 2023/2/20
 */
public class StringPartitioner extends Partitioner {

    private final int numPartitions;

    public StringPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        String item = (String) key;
        int partition = item.charAt(0) % numPartitions;

        return partition;
    }
}
