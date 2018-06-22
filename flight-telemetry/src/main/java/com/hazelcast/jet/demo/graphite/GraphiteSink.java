package com.hazelcast.jet.demo.graphite;

import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.pipeline.Sink;
import com.hazelcast.jet.pipeline.Sinks;
import org.python.core.PyString;
import org.python.modules.cPickle;

import java.io.BufferedOutputStream;
import java.net.Socket;
import java.nio.ByteBuffer;

import static com.hazelcast.jet.impl.util.Util.uncheckCall;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;

public class GraphiteSink {

    /**
     * Sink implementation which forwards the items it receives to the Graphite.
     * Graphite's Pickle Protocol is used for communication between Jet and Graphite.
     *
     * @param host Graphite host
     * @param port Graphite port
     */
    public static Sink<TimestampedEntry> build(String host, int port) {
        return Sinks.<BufferedOutputStream, TimestampedEntry>builder(instance -> uncheckCall(()
                -> new BufferedOutputStream(new Socket(host, port).getOutputStream())
        ))
                .onReceiveFn((bos, entry) -> uncheckRun(() -> {
                    GraphiteMetric metric = new GraphiteMetric();
                    metric.from(entry);

                    PyString payload = cPickle.dumps(metric.getAsList(), 2);
                    byte[] header = ByteBuffer.allocate(4).putInt(payload.__len__()).array();

                    bos.write(header);
                    bos.write(payload.toBytes());
                }))
                .flushFn((bos) -> uncheckRun(bos::flush))
                .destroyFn((bos) -> uncheckRun(bos::close))
                .build();
    }

}
