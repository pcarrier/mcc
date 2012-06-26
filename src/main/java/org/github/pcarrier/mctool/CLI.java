package org.github.pcarrier.mctool;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Meter;
import com.yammer.metrics.reporting.ConsoleReporter;
import lombok.Data;
import lombok.Getter;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import net.rubyeye.xmemcached.KeyIterator;
import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClient;
import net.rubyeye.xmemcached.exception.MemcachedException;
import net.rubyeye.xmemcached.utils.AddrUtil;
import org.iq80.cli.*;
import org.iq80.cli.Cli.CliBuilder;
import org.msgpack.MessagePack;
import org.msgpack.packer.Packer;

import java.io.BufferedOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.iq80.cli.Cli.buildCli;

@Slf4j
public class CLI {
    public static void main(String[] args) {
        ConsoleReporter.enable(1, TimeUnit.SECONDS);

        CliBuilder<Runnable> builder = buildCli("mctool", Runnable.class)
                .withDescription("memcached client tool")
                .withDefaultCommand(Help.class)
                .withCommands(Help.class, Dump.class, Restore.class);

        Cli<Runnable> mcParser = builder.build();
        mcParser.parse(args).run();
    }

    public static class McCommand implements Runnable {
//        @Option(type = OptionType.GLOBAL, name = "-v", description = "Verbose mode")
//        public boolean verbose;

        @Override
        public void run() {
            System.out.println(getClass().getSimpleName());
            /* Decrease useless logging */
        }
    }

    @Command(name = "dump", description = "Dump to a file")
    public static class Dump extends McCommand {
        boolean doneDumpingKeys = false;

        @Option(name = "-s",
                description = "Server to dump from (defaults to 127.0.0.1)")
        public String server = "127.0.0.1:11211";

        @Option(name = "-c",
                description = "Key coalescence (defaults to 1024)")
        public int coalescence = 4096;

        @Option(name = "-q",
                description = "Queue capacity (defaults to 1024)")
        public int capacity = 4096;

        @Option(name = "-t",
                description = "Dump threads (defaults to 4)")
        public int threads = 4;

        @Arguments(description = "Destination path (defaults to \"dump\")")
        public String path = "dump";

        protected Packer packer;

        @Data
        private class KeyspaceWorker implements Runnable {
            @NonNull
            String server;
            @NonNull
            BlockingQueue<List<String>> target;
            @NonNull
            Integer coalescence;

            final Meter keysRead = Metrics.newMeter(KeyspaceWorker.class, "keys-read", "reads", TimeUnit.SECONDS);

            @Override
            public void run() {
                try {
                    final InetSocketAddress addr = AddrUtil.getOneAddress(server);
                    final XMemcachedClient client = new XMemcachedClient(addr);
                    KeyIterator iterator = client.getKeyIterator(AddrUtil.getOneAddress(server));

                    while (iterator.hasNext()) {
                        List<String> chunk = new ArrayList<String>(coalescence);
                        while (iterator.hasNext() && chunk.size() < coalescence) {
                            chunk.add(iterator.next());
                            keysRead.mark();
                        }
                        target.put(chunk);
                    }

                    client.shutdown();
                } catch (IOException | InterruptedException | MemcachedException | TimeoutException e) {
                    log.error("couldn't iterate through keys");
                    System.exit(-5);
                } finally {
                    doneDumpingKeys = true;
                }
            }
        }

        @Data
        private class DumpWorker implements Runnable {
            @NonNull
            String server;
            @NonNull
            BlockingQueue<List<String>> source;

            final Meter valuesRead = Metrics.newMeter(KeyspaceWorker.class, "values-read", "reads", TimeUnit.SECONDS);

            @Override
            public void run() {
                final InetSocketAddress addr = AddrUtil.getOneAddress(server);

                try {
                    XMemcachedClient client = new XMemcachedClient(addr);

                    while (!doneDumpingKeys || !source.isEmpty()) {
                        List<String> keys = source.poll();

                        if (keys == null) {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                                log.warn("couldn't wait!");
                            }
                        } else {
                            Map<String, String> map = null;
                            try {
                                map = client.get(keys);
                            } catch (InterruptedException | MemcachedException | TimeoutException failed_to_get_keys) {
                                log.warn("failed to grab keys");
                            } finally {
                                if (map != null) {
                                    try {
                                        synchronized (packer) {
                                            for (Map.Entry<String, String> entry : map.entrySet()) {
                                                packer.write(entry.getKey().getBytes());
                                                packer.write(entry.getValue().getBytes());
                                            }
                                        }
                                    } catch (IOException e) {
                                        log.error("writing failed");
                                        System.exit(-3);
                                    }
                                }
                            }
                        }
                    }

                    client.shutdown();
                } catch (IOException e) {
                    log.error("couldn't create memcached client");
                    System.exit(-4);
                }
            }
        }

        @Override
        public void run() {
            super.run();

            final BlockingQueue<List<String>> queue = new LinkedBlockingQueue<List<String>>(capacity);
            final LinkedList<Thread> threadList = new LinkedList<Thread>();

            try (OutputStream outputStream = new BufferedOutputStream(new FileOutputStream(path))) {
                final MessagePack msgPack = new MessagePack();
                packer = msgPack.createPacker(outputStream);

                final Thread keyspaceThread = new Thread(new KeyspaceWorker(server, queue, coalescence));
                threadList.add(keyspaceThread);
                keyspaceThread.start();

                for (int i = 0; i < threads; i++) {
                    final Thread workerThread = new Thread(new DumpWorker(server, queue));
                    threadList.add(workerThread);
                    workerThread.start();
                }

                for (Thread t : threadList) {
                    try {
                        t.join();
                    } catch (InterruptedException e1) {
                        log.warn("couldn't join a thread");
                    }
                }
            } catch (IOException e) {
                log.error("couldn't create the dump file");
                System.exit(-2);
            }
        }
    }

    @Command(name = "restore", description = "Restore a dump")
    public static class Restore extends CLI.McCommand {
        @Option(name = "-s",
                description = "Weighted list of servers (defaults to \"127.0.0.1:11211,1\"")
        public String servers = "127.0.0.1,1";

        @Getter(lazy = true)
        private final MemcachedClient client = ArgumentParsing.buildClient(servers);

        @Override
        public void run() {
            super.run();
//            getClient(); /* for debugging purposes */
        }
    }
}
