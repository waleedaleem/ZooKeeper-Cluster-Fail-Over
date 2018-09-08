package com.walid.curator;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.CuratorListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

public class CheckDisconnApp implements CuratorListener {

    private static final String CHECK_PATH = "/CMTest/leader";
    private static final Logger logger = LoggerFactory.getLogger(CheckDisconnApp.class);
    private static final String ZK_CONN_STRING = "127.0.0.1:2181";
    private static final CuratorFramework CURATOR_FRAMEWORK = CuratorFrameworkFactory.newClient(
            ZK_CONN_STRING, new ExponentialBackoffRetry(1000, 3));
    private static String clientId;

    public static void main(String[] args) throws Exception {
        if (args.length != 0) clientId = args[0];
        CURATOR_FRAMEWORK.getCuratorListenable().addListener(new CheckDisconnApp());
        CURATOR_FRAMEWORK.start();
        for (; ; ) {
            try {
                final Stat stat = CURATOR_FRAMEWORK.checkExists().watched().forPath(CHECK_PATH);
                if (stat != null) {
                    break;
                } else {
                    Thread.sleep(500);
                }
            } catch (Exception ex) {
                logger.debug("Exception caught", ex);
            }
        }

        final List<String> children = CURATOR_FRAMEWORK.getChildren().watched().forPath(CHECK_PATH);
        if (children.contains(clientId)) {
            disconnect();
        }

        System.in.read();
    }

    private static void disconnect() throws IOException, InterruptedException {
        new ProcessBuilder("tcpkill port 2181 > /dev/null 2>&1 &").start();
        Thread.sleep(1000);
        new ProcessBuilder("pkill -9 tcpkill").start();
    }

    @Override
    public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
        logger.debug("event = [{}]", event);
        String eventPath = event.getPath();
        WatchedEvent watchedEvent = event.getWatchedEvent();
        if (eventPath == null || watchedEvent == null) {
            return;
        }

        if (CHECK_PATH.equals(eventPath)
                && watchedEvent.getType().equals(Watcher.Event.EventType.NodeChildrenChanged)
                && (CURATOR_FRAMEWORK.checkExists().watched().forPath(CHECK_PATH) != null)) {
            final List<String> children = CURATOR_FRAMEWORK.getChildren().watched().forPath(CHECK_PATH);
            logger.debug("Current leaders {}", children);
            if (children.contains(clientId)) disconnect();
        }
    }
}
