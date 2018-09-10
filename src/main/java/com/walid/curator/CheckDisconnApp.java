package com.walid.curator;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.base.Joiner;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class CheckDisconnApp implements CuratorListener {

    private static final Logger logger = LoggerFactory.getLogger(CheckDisconnApp.class);
    private static final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1,
            new ThreadFactoryBuilder().setNameFormat("Scheduled Leader Monitor").build());
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
            System.exit(1);
        }

        checkDisconnApp.run();

        System.out.println("Hit Return to quit.");
        System.in.read();
    }

    private void run() throws Exception {
        curatorFramework = CuratorFrameworkFactory.newClient(
                zkConnString, new ExponentialBackoffRetry(1000, 50));
        curatorFramework.start();
        logger.debug("Connected to Zookeeper server at [{}]", zkConnString);
        logger.debug("Waiting for leader election path [{}] to exist on Zookeeper", checkPath);
        for (; ; ) {
            try {
                final Stat stat = curatorFramework.checkExists().forPath(checkPath);
                if (stat != null) {
                    System.out.printf("%n");
                    break;
                } else {
                    System.out.print(".");
                    Thread.sleep(500);
                }
            } catch (Exception ex) {
                logger.warn("Exception caught: ", ex);
            }
        }
        curatorFramework.getCuratorListenable().addListener(this);
        final List<String> children = curatorFramework.getChildren().watched().forPath(checkPath);
        if (children.isEmpty()) {
            logger.debug("No latches found yet");
        } else {
            logger.debug("Found latches [{}]", Joiner.on(", ").join(children));
            checkLeader();
        }

        // kick off the leader monitor thread
        scheduler.scheduleAtFixedRate(new LeaderMonitor(Thread.currentThread()), 1000, 500, TimeUnit.MILLISECONDS);
    }

    private void disconnect() throws IOException {
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
        try {
            Thread.sleep(disconnDurationMs);
        } catch (InterruptedException e) {
            logger.info("Mission accomplished...Exiting!");
            System.exit(0);
        }
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
            checkLeader();
        }
    }

    private void checkLeader() throws Exception {
        if (curatorFramework.checkExists().forPath(checkPath) != null) {
            final List<String> children = curatorFramework.getChildren().watched().forPath(checkPath);

            if (children.isEmpty()) {
                logger.info("Current leader [None]");
                return;
            }

            String leaderLatch = children.get(0);
            for (String latch : children) {
                if (latch.split("-latch-")[1].compareTo(leaderLatch.split("-latch-")[1]) < 0) {
                    leaderLatch = latch;
                }
            }
            String winnerLatchData = new String(curatorFramework.getData().forPath(checkPath + "/" + leaderLatch));
            logger.debug("Winning latch [{}]", winnerLatchData);
            if (winnerLatchData.equalsIgnoreCase(clientId)) disconnect();
        } else {
            logger.debug("Leader base zNode [{}] removed. Current leader [None]", checkPath);
        }
    }

    /**
     * Monitors the children of /CMTest/leader and acts on void leadership
     */
    private class LeaderMonitor implements Runnable {

        private final String leaderPath = checkPath.replaceFirst("-election$", "");
        private final Thread mainThread;
        private List<String> currentLeaders = new ArrayList<>();

        LeaderMonitor(Thread mainThread) {
            logger.info("Kicking off leader monitor at path [{}]", leaderPath);
            this.mainThread = mainThread;
        }

        @Override
        public void run() {
            try {
                List<String> leaders = curatorFramework.getChildren().watched().forPath(leaderPath);
                if (!currentLeaders.isEmpty() && leaders.isEmpty()) {
                    logger.debug("No leader at the moment!!!");
                    mainThread.interrupt();
                    logger.info("Monitor mission accomplished...Exiting!");
                    System.exit(0);
                } else if (currentLeaders.isEmpty() && !leaders.isEmpty()
                        || !leaders.isEmpty() && !leaders.equals(currentLeaders)) {
                    logger.debug("Current leader is {}", leaders);
                }
                currentLeaders = leaders;
            } catch (Exception e) {
                logger.error("Error retrieving leader Id from ZK", e);
            }
        }
    }
}
