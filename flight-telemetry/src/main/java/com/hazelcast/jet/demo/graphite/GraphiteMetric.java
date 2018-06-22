package com.hazelcast.jet.demo.graphite;

import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.demo.types.Aircraft;
import org.python.core.*;

import java.time.Instant;

/**
 * A data transfer object for Graphite
 */
public class GraphiteMetric {

    private PyString metricName;
    private PyInteger timestamp;
    private PyFloat metricValue;

    public GraphiteMetric() {
    }

    private void fromAirCraftEntry(TimestampedEntry<Long, Aircraft> aircraftEntry) {
        Aircraft aircraft = aircraftEntry.getValue();
        metricName = new PyString(replaceWhiteSpace(aircraft.getAirport()) + "." + aircraft.getVerticalDirection());
        timestamp = new PyInteger(getEpochSecond(aircraft.getPosTime()));
        metricValue = new PyFloat(1);
    }

    private void fromMaxNoiseEntry(TimestampedEntry<String, Integer> entry) {
        metricName = new PyString(replaceWhiteSpace(entry.getKey()));
        timestamp = new PyInteger(getEpochSecond(entry.getTimestamp()));
        metricValue = new PyFloat(entry.getValue());
    }

    private void fromTotalC02Entry(TimestampedEntry<String, Double> entry) {
        metricName = new PyString(replaceWhiteSpace(entry.getKey()));
        timestamp = new PyInteger(getEpochSecond(entry.getTimestamp()));
        metricValue = new PyFloat(entry.getValue());
    }

    public void from(TimestampedEntry entry) {
        if (entry.getKey() instanceof Long) {
            TimestampedEntry<Long, Aircraft> aircraftEntry = entry;
            fromAirCraftEntry(aircraftEntry);
        } else {
            if (entry.getValue() instanceof Double) {
                fromTotalC02Entry(entry);
            } else {
                fromMaxNoiseEntry(entry);
            }
        }
    }

    public PyList getAsList() {
        PyList list = new PyList();
        PyTuple metric = new PyTuple(metricName, new PyTuple(timestamp, metricValue));
        list.add(metric);
        return list;
    }

    private int getEpochSecond(long millis) {
        return (int) Instant.ofEpochMilli(millis).getEpochSecond();
    }

    private String replaceWhiteSpace(String string) {
        return string.replace(" ", "_");
    }
}
