package org.apache.dolphinscheduler.client.zookeeper;

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.Charset;

/**
 * @Author: Tboy
 */
@Component
public class ZookeeperCachedOperator extends ZookeeperOperator {

    private final Logger logger = LoggerFactory.getLogger(ZookeeperCachedOperator.class);

    private TreeCache treeCache;

    public void start(String cachePath){
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
