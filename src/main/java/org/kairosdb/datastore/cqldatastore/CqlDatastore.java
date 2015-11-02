package org.kairosdb.datastore.cqldatastore;

import com.google.common.collect.ImmutableSortedMap;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import me.prettyprint.hector.api.Keyspace;
import org.joda.time.DateTime;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.KairosDataPointFactory;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.core.datastore.DatastoreMetricQuery;
import org.kairosdb.core.datastore.QueryCallback;
import org.kairosdb.core.datastore.TagSet;
import org.kairosdb.core.exception.DatastoreException;
import org.kairosdb.datastore.cassandra.*;

import com.datastax.driver.core.*;
import org.kairosdb.util.KDataOutput;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

/**
 * Created by ranbirc on 10/31/15.
 */
public class CqlDatastore implements Datastore {

    public static final Logger logger = LoggerFactory.getLogger(CassandraDatastore.class);

    public static final int LONG_FLAG = 0x0;
    public static final int FLOAT_FLAG = 0x1;

    public static final DataPointsRowKeySerializer DATA_POINTS_ROW_KEY_SERIALIZER = new DataPointsRowKeySerializer();


    public static final long ROW_WIDTH = 1814400000L; //3 Weeks wide

    public static final String KEY_QUERY_TIME = "kairosdb.datastore.cassandra.key_query_time";


    public static final String CF_DATA_POINTS = "data_points";
    public static final String CF_ROW_KEY_INDEX = "row_key_index";
    public static final String CF_STRING_INDEX = "string_index";

    public static final String ROW_KEY_METRIC_NAMES = "metric_names";
    public static final String ROW_KEY_TAG_NAMES = "tag_names";
    public static final String ROW_KEY_TAG_VALUES = "tag_values";

    /* CQL Specific */
    private Cluster m_cluster;
    private Session m_session = null;
    private PreparedStatement insertMetricStmt;

    /* end CQL Specific */
    private Keyspace m_keyspace;
    private String m_keyspaceName;
    private int m_singleRowReadSize;
    private int m_multiRowSize;

    private int m_multiRowReadSize;
    private WriteBuffer<DataPointsRowKey, Integer, byte[]> m_dataPointWriteBuffer;
    private WriteBuffer<String, DataPointsRowKey, String> m_rowKeyWriteBuffer;
    private WriteBuffer<String, String, String> m_stringIndexWriteBuffer;

    private DataCache<DataPointsRowKey> m_rowKeyCache = new DataCache<DataPointsRowKey>(1024);
    private DataCache<String> m_metricNameCache = new DataCache<String>(1024);
    private DataCache<String> m_tagNameCache = new DataCache<String>(1024);
    private DataCache<String> m_tagValueCache = new DataCache<String>(1024);

//    private final KairosDataPointFactory m_kairosDataPointFactory;

    private CassandraConfiguration m_cassandraConfiguration;


    @Inject
    public CqlDatastore(@Named("HOSTNAME") final String hostname,
                        CassandraConfiguration cassandraConfiguration,
                        DataStaxConfiguration configuration,
                        KairosDataPointFactory kairosDataPointFactory) throws DatastoreException
    {
        String hostList = configuration.getHostlist();
        m_cluster = Cluster.builder().addContactPoints(hostList).build();
        Metadata metadata = m_cluster.getMetadata();
        System.out.printf("Connected to cluster: %s\n",
                metadata.getClusterName());
        for ( Host host : metadata.getAllHosts() ) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        m_session = m_cluster.connect();

        //create keyspace if it doesn't exist
        int replicatonfactor = cassandraConfiguration.getReplicationFactor();
        String keyspaceName = cassandraConfiguration.getKeyspaceName();

        String createKeySpaceStmt = String.format("CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class':'SimpleStrategy', 'replication_factor': %d};",keyspaceName,replicatonfactor);
        m_session.execute(createKeySpaceStmt);

        String createRowTableStmt = String.format("CREATE TABLE IF NOT EXISTS %s.%s (metric_id text,event_time timestamp,row_key int,tags text,data_type text,value blob,PRIMARY KEY((metric_id,row_key,tags),event_time)) WITH CLUSTERING ORDER BY (event_time DESC);",keyspaceName,CF_DATA_POINTS);
        m_session.execute(createRowTableStmt);

        insertMetricStmt = m_session.prepare("INSERT INTO kairosdb.data_points(metric_id,event_time,row_key,tags,data_type,value) VALUES(?,?,?,?,?,?) using TTL ?");
        /* 


        CREATE TABLE IF NOT EXISTS data_points (
            metric_id text,
            event_time timestamp,
            row_key int,
            tags text,
            data_type text,
            value blob,
            PRIMARY KEY((metric_id,row_key,tags),event_time)
        ) WITH CLUSTERING ORDER BY (event_time DESC);

        INSERT INTO data_points(metric_id,event_time,row_key,tags,data_type,value) VALUES ('com.test.metric',1445688000000,201543,'tag1:val1','double','99.5') using TTL 600;
        INSERT INTO data_points(metric_id,event_time,row_key,tags,data_type,value) VALUES ('com.test.metric',1446292740000,201544,'tag1:val1','double','100.5') using TTL 600;
        INSERT INTO data_points(metric_id,event_time,row_key,tags,data_type,value) VALUES ('com.test.metric',1446464700000,201545,'tag1:val1','double','101.5') using TTL 600;
        INSERT INTO data_points(metric_id,event_time,row_key,tags,data_type,value) VALUES ('com.test.metric',1447124400000,201546,'tag1:val1','double','105.5') using TTL 600;

        select * from data_points where metric_id = 'com.test.metric' and event_time > 1445688000000 and event_time < 1447124400000;
        select metric_id,event_time,data_type,value from data_points where metric_id = 'com.test.metric' and tags = 'tag1:val1' and event_time > 1445688000000 and event_time < 1447124400000 and row_key in (201543,201544,201545) order by event_time;

        select metric_id,event_time,data_type,value from data_points where metric_id = 'com.test.metric' and tags = 'tag1:val1' and event_time > 1445688000000 and event_time < 1447124400000 and row_key = 201544 order by event_time;

        select * from data_points where  tags = 'tag1:val1' and metric_id = 'com.test.metric' and row_key in (43,44,45) ;


        UPDATE data_points SET row_key = 55 where value = '103';
1446464700
         */


    }

    public void putDataPoint(String metricName, ImmutableSortedMap<String, String> tags, DataPoint dataPoint, int ttl) throws DatastoreException {

        try {

            //create new concate string of tag names - not sure we need to check cache since we
            //write it as a seperate cql table element now
            StringBuffer tagBuffer = new StringBuffer();

            for(String tagName : tags.keySet()) {
                if(tagName.length() == 0)
                {
                    logger.warn(
                            "Attempted to add empty tagName to string cache for metric: "+metricName
                    );
                }
                tagBuffer.append(tagName).append(':').append(tags.get(tagName)).append("|");
            }
            String tagStr = tagBuffer.substring(0,tagBuffer.length()-1);

            //will call row__key later

            DateTime dateTime = new DateTime(dataPoint.getTimestamp());
            int rowId = (dateTime.getWeekOfWeekyear()/3)+(dateTime.getYear()*100);

            KDataOutput kDataOutput = new KDataOutput();
            dataPoint.writeValueToBuffer(kDataOutput);
            ByteBuffer byteBuffer = ByteBuffer.wrap(kDataOutput.getBytes());
            ;

            m_session.execute(insertMetricStmt.bind(metricName,dateTime.toDate(), rowId, tagStr, dataPoint.getApiDataType(),byteBuffer,0));

        }
            catch (Exception e)
            {
                throw new DatastoreException(e);
            }

    }

    public void close() throws InterruptedException, DatastoreException {
        if(m_cluster != null) {
            m_cluster.close();
        }
    }

    public Iterable<String> getMetricNames() throws DatastoreException {
        return null;
    }

    public Iterable<String> getTagNames() throws DatastoreException {
        return null;
    }

    public Iterable<String> getTagValues() throws DatastoreException {
        return null;
    }

    public void queryDatabase(DatastoreMetricQuery query, QueryCallback queryCallback) throws DatastoreException {

    }

    public void deleteDataPoints(DatastoreMetricQuery deleteQuery) throws DatastoreException {

    }

    public TagSet queryMetricTags(DatastoreMetricQuery query) throws DatastoreException {
        return null;
    }

    /*





     */


}
