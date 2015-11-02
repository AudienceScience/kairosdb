package org.kairosdb.datastore.cqldatastore;

import com.google.inject.Inject;
import com.google.inject.name.Named;

/**
 * Created by ranbirc on 11/1/15.
 */
public class DataStaxConfiguration {

    private static final String HOST_LIST_PROPERTY = "kairosdb.datastore.cassandra.host_list";

    private String hostlist;

    @Inject
    public DataStaxConfiguration(@Named(HOST_LIST_PROPERTY) String cassandraHostList)
    {
        hostlist = cassandraHostList;
    }

    public String getHostlist() {
        return hostlist;
    }

    public void setHostlist(String hostlist) {
        this.hostlist = hostlist;
    }
}
