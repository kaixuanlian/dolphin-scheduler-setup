package org.apache.dolphinscheduler.client.zookeeper;

import org.apache.curator.ensemble.EnsembleProvider;

import java.io.IOException;

/**
 * @Author: Tboy
 */
public class DefaultEnsembleProvider implements EnsembleProvider {

    private final String serverList;

    public DefaultEnsembleProvider(String serverList){
        this.serverList = serverList;
    }

    @Override
    public void start() throws Exception {
        //NOP
    }

    @Override
    public String getConnectionString() {
        return serverList;
    }

    @Override
    public void close() throws IOException {
        //NOP
    }
}
