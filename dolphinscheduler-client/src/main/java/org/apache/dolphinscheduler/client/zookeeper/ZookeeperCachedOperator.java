package org.apache.dolphinscheduler.client.zookeeper;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;

/**
 * @Author: Tboy
 */
public class ZookeeperCachedOperator extends ZookeeperOperator {

    private final Logger logger = LoggerFactory.getLogger(ZookeeperCachedOperator.class);

    private final TreeCache treeCache;

    public ZookeeperCachedOperator(CuratorFramework client, String cachePath){
        super(client);
        this.treeCache = new TreeCache(client, cachePath);
        try {
            this.treeCache.start();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public String getFromCache(final String key) {
        ChildData resultInCache = treeCache.getCurrentData(key);
        if (null != resultInCache) {
            return null == resultInCache.getData() ? null : new String(resultInCache.getData(), Charset.forName("UTF-8"));
        }
        return null;
    }

    public TreeCache getTreeCache() {
        return treeCache;
    }

    public void addListener(TreeCacheListener listener){
        this.treeCache.getListenable().addListener(listener);
    }

    public void close(){
        treeCache.close();
    }
}
