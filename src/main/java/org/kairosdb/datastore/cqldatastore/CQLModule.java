package org.kairosdb.datastore.cqldatastore;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.kairosdb.core.datastore.Datastore;
import org.kairosdb.datastore.cassandra.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * Created by ranbirc on 11/1/15.
 */
public class CQLModule extends AbstractModule
{
    public static final String CASSANDRA_AUTH_MAP = "cassandra.auth.map";
    public static final String CASSANDRA_HECTOR_MAP = "cassandra.hector.map";
    public static final String AUTH_PREFIX = "kairosdb.datastore.cassandra.auth.";
    public static final String HECTOR_PREFIX = "kairosdb.datastore.cassandra.hector.";

    private Map<String, String> m_authMap = new HashMap<String, String>();
    private Map<String, Object> m_hectorMap = new HashMap<String, Object>();

    public CQLModule(Properties props)
    {
        for (Object key : props.keySet())
        {
            String strKey = (String)key;

            if (strKey.startsWith(AUTH_PREFIX))
            {
                String consumerKey = strKey.substring(AUTH_PREFIX.length());
                String consumerToken = (String)props.get(key);

                m_authMap.put(consumerKey, consumerToken);
            }
            else if (strKey.startsWith(HECTOR_PREFIX))
            {
                String configKey = strKey.substring(HECTOR_PREFIX.length());
                m_hectorMap.put(configKey, props.get(key));
            }
        }
    }


    @Override
    protected void configure()
    {
        bind(Datastore.class).to(CqlDatastore.class).in(Scopes.SINGLETON);
        bind(CqlDatastore.class).in(Scopes.SINGLETON);
//        bind(IncreaseMaxBufferSizesJob.class).in(Scopes.SINGLETON);
//        bind(CleanRowKeyCache.class).in(Scopes.SINGLETON);
        bind(DataStaxConfiguration.class).in(Scopes.SINGLETON);
        bind(CassandraConfiguration.class).in(Scopes.SINGLETON);

        bind(new TypeLiteral<List<RowKeyListener>>(){}).toProvider(RowKeyListenerProvider.class);

        bind(new TypeLiteral<Map<String, String>>(){}).annotatedWith(Names.named(CASSANDRA_AUTH_MAP))
                .toInstance(m_authMap);

        bind(new TypeLiteral<Map<String, Object>>(){}).annotatedWith(Names.named(CASSANDRA_HECTOR_MAP))
                .toInstance(m_hectorMap);
    }
}
