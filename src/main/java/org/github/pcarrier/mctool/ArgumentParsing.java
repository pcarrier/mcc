package org.github.pcarrier.mctool;

import com.google.common.base.Splitter;
import com.google.common.primitives.Ints;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;
import net.rubyeye.xmemcached.MemcachedClient;
import net.rubyeye.xmemcached.XMemcachedClientBuilder;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.LinkedList;

import static com.google.common.collect.Iterables.toArray;

@Slf4j
public class ArgumentParsing {
    static public @NonNull MemcachedClient buildClient(String weightedList) {
        final LinkedList<InetSocketAddress> socketAddresses = new LinkedList<InetSocketAddress>();
        final LinkedList<Integer> weights = new LinkedList<Integer>();

        for (String weightedSpec : Splitter.on(" ").trimResults().omitEmptyStrings().split(weightedList)) {
            int port = 11211;

            final String[] weightedSpecsParts = toArray(Splitter.on(",").split(weightedSpec), String.class);
            if(weightedSpecsParts.length != 2)
                throw new RuntimeException("Weight required");
            int weight = Integer.parseInt(weightedSpecsParts[1]);

            String remote = weightedSpecsParts[0];
            final String[] remoteParts = toArray(Splitter.on(":").split(remote), String.class);
            String host = remoteParts[0];
            if (remoteParts.length > 1)
                port = Integer.parseInt(remoteParts[1]);

            log.info(String.format("Using server %s:% with weight %d", host, port, weight));

            socketAddresses.add(new InetSocketAddress(host, port));
            weights.add(weight);
        }

        final XMemcachedClientBuilder builder = new XMemcachedClientBuilder(socketAddresses, Ints.toArray(weights));
        builder.setSanitizeKeys(false);

        try {
            final MemcachedClient client = builder.build();
            client.setPrimitiveAsString(true);

            return client;
        } catch (IOException e) {
            /* Annoyingly not supported by Lombok's lazy getters (at least with IntelliJ) */
            throw new RuntimeException(e);
        }
    }
}
