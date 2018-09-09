package com.walid.curator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
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

import java.io.File;
import java.io.IOException;
import java.util.List;

public class CheckDisconnApp implements CuratorListener {

    private static final Logger logger = LoggerFactory.getLogger(CheckDisconnApp.class);
    @Parameter(names = {"--zk-connection", "-z"}, description = "Description: ZK connection string")
    private String zkConnString = "127.0.0.1:2181";
    @Parameter(names = {"--zk-leader-path", "-l"}, description = "Description: ZK leader base path")
    private String checkPath = "/CMTest/leader-election";
    @Parameter(names = {"--disconn-duration", "-d"}, description = "Description: Configurable connection disruption duration")
    private int disconnDurationMs = 5000;
    @Parameter(names = {"--client-id", "-i"}, description = "Description: ZK client ID", required = true)
    private String clientId = "client1";
    private CuratorFramework curatorFramework;

    public static void main(String[] args) throws Exception {
        CheckDisconnApp checkDisconnApp = new CheckDisconnApp();
        try {
            JCommander.newBuilder()
                    .addObject(checkDisconnApp)
                    .build()
                    .parse(args);
        } catch (ParameterException pex) {
            pex.usage();
        }

        checkDisconnApp.run();

        System.out.println("Hit Return to quit.");
        System.in.read();
    }

    private void run() throws Exception {
        curatorFramework = CuratorFrameworkFactory.newClient(
                zkConnString, new ExponentialBackoffRetry(1000, 50));
        curatorFramework.start();
        for (; ; ) {
            try {
                final Stat stat = curatorFramework.checkExists().forPath(checkPath);
                if (stat != null) {
                    break;
                } else {
                    Thread.sleep(500);
                }
            } catch (Exception ex) {
                logger.debug("Exception caught: ", ex);
            }
        }
        curatorFramework.getCuratorListenable().addListener(this);
        final List<String> children = curatorFramework.getChildren().watched().forPath(checkPath);
        if (children.contains(clientId)) {
            disconnect();
        }
    }

    private void disconnect() throws IOException, InterruptedException {
        logger.info("Disrupting \"{}\" connection to host \"{}\" for {} milliseconds", clientId, zkConnString, disconnDurationMs);
        int zkPort;
        try {
            zkPort = Integer.parseInt(zkConnString.split(":")[1]);
        } catch (NumberFormatException nfe) {
            zkPort = 2181;
        }
        logger.debug("Starting disconnect process");
        new ProcessBuilder("/bin/bash", "-c", String.format("tcpkill port %d", zkPort))
                .directory(new File("/"))
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(new File("process.log")))
                .start();
        Thread.sleep(disconnDurationMs);
        logger.debug("Stopping disconnect process");
        new ProcessBuilder("/bin/bash", "-c", "pkill -9 tcpkill")
                .directory(new File("/"))
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(new File("process.log")))
                .start();
        logger.debug("Disrupted \"{}\" connection to host \"{}\" for {} milliseconds", clientId, zkConnString, disconnDurationMs);
    }

    @Override
    public void eventReceived(CuratorFramework client, CuratorEvent event) throws Exception {
        logger.debug("event = [{}]", event);
        String eventPath = event.getPath();
        WatchedEvent watchedEvent = event.getWatchedEvent();
        if (eventPath == null || watchedEvent == null) {
            return;
        }

        if (checkPath.equals(eventPath)
                && watchedEvent.getType().equals(Watcher.Event.EventType.NodeChildrenChanged)) {
            if (curatorFramework.checkExists().forPath(checkPath) != null) {
                final List<String> children = curatorFramework.getChildren().watched().forPath(checkPath);
                logger.debug("Current leaders {}", children);
                if (children.contains(clientId)) disconnect();
            } else {
                logger.debug("Leader base zNode [{}] removed. Current leaders []", checkPath);
            }
        }
    }
}
