package com.hazelcast.jet.demo;

import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Job;
import com.hazelcast.jet.datamodel.TimestampedEntry;
import com.hazelcast.jet.demo.graphite.GraphiteSink;
import com.hazelcast.jet.demo.types.Aircraft;
import com.hazelcast.jet.demo.types.Aircraft.VerticalDirection;
import com.hazelcast.jet.pipeline.GeneralStage;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.jet.pipeline.StreamStage;

import java.util.List;
import java.util.Map.Entry;
import java.util.SortedMap;

import static com.hazelcast.jet.Util.entry;
import static com.hazelcast.jet.aggregate.AggregateOperations.*;
import static com.hazelcast.jet.demo.Constants.typeToLTOCycyleC02Emission;
import static com.hazelcast.jet.demo.types.Aircraft.VerticalDirection.*;
import static com.hazelcast.jet.demo.util.Util.getAirport;
import static com.hazelcast.jet.demo.util.Util.getPhaseNoiseLookupTable;
import static com.hazelcast.jet.function.DistributedComparator.comparingInt;
import static com.hazelcast.jet.pipeline.WindowDefinition.sliding;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Reads a ADB-S telemetry stream from [ADB-S Exchange](https://www.adsbexchange.com/)
 * on all commercial aircraft flying anywhere in the world.
 * The service provides real-time information about flights.
 *
 * Flights in the low altitudes are filtered for determining whether
 * they are ascending or descending. This has been done with calculation of
 * linear trend of altitudes. After vertical direction determination, ascending
 * and descending flight are written to a Hazelcast Map.
 *
 * For interested airports, the pipeline also calculates average C02 emission
 * and maximum noise level.
 * C02 emission and maximum noise level are calculated by enriching the data
 * stream with average landing/take-off emissions and noise levels at the
 * specific altitudes for airplanes models and categories which can be found
 * on {@link Constants}.
 *
 * After all those calculations results are forwarded to a Graphite
 * metrics storage which feds the Grafana Dashboard.
 *
 * The DAG used to model Flight Telemetry calculations can be seen below :
 *
 *                                                  ┌──────────────────┐
 *                                                  │Flight Data Source│
 *                                                  └─────────┬────────┘
 *                                                            │
 *                                                            v
 *                                                   ┌─────────────────┐
 *                                                   │Insert Watermarks│
 *                                                   └────────┬────────┘
 *                                                            │
 *                                                            v
 *                                           ┌────────────────────────────────┐
 *                                           │Filter Aircraft in Low Altitudes│
 *                                           └────────────────┬───────────────┘
 *                                                            │
 *                                                            v
 *                                                  ┌───────────────────┐
 *                                                  │Assign Airport Info│
 *                                                  └─────────┬─────────┘
 *                                                            │
 *                                                            v
 *                                          ┌───────────────────────────────────┐
 *                                          │Calculate Linear Trend of Altitudes│
 *                                          └─────────────────┬─────────────────┘
 *                                                            │
 *                                                            v
 *                                               ┌─────────────────────────┐
 *                                               │Assign Vertical Direction│
 *                                               └──────────┬──┬──┬────────┘
 *                                                          │  │  │
 *                                                      ┌───┘  │  └─────┐
 *                                                      │      │        │
 *                                                      │      │        │
 *                                                      v      │        v
 *                                    ┌────────────────────┐   │  ┌──────────────────────┐
 *                                    │Enrich with C02 Info│   │  │Enrich with Noise Info│
 *                                    └────────────────────┘   │  └──────────────────────┘
 *                                                      │      │        │
 *                                                      │      │        │
 *                                                      v      │        v
 *                                 ┌───────────────────────┐   │    ┌─────────────────────────┐
 *                                 │Calculate Avg C02 Level│   │    │Calculate Max Noise Level│
 *                                 └────────────────────┬──┘   │    └───────────────┬─────────┘
 *                                                      │      │    ┌───────────────┘
 *                                                      │      │    │
 *                                                      │      │    │
 *                                                      v      v    v
 *                                                    ┌───────────────┐
 *                                                    │ Graphite Sink │
 *                                                    └───────────────┘
 *
 *
 *
 */
public class FlightTelemetry {

    private static final String SOURCE_URL = "https://public-api.adsbexchange.com/VirtualRadar/AircraftList.json";

    /**
     * Builds and returns the Pipeline which represents the actual computation.
     */
    private static Pipeline buildPipeline() {
        Pipeline pipeline = Pipeline.create();

        // Filter aircrafts whose altitude less then 3000ft, calculate linear trend of their altitudes
        // and assign vertical directions to the aircrafts.
        StreamStage<TimestampedEntry<Long, Aircraft>> flights = pipeline
                .drawFrom(FlightDataSource.streamAircraft(SOURCE_URL, 10000))
                .addTimestamps(Aircraft::getPosTime, SECONDS.toMillis(15))
                .filter(a -> !a.isGnd() && a.getAlt() > 0 && a.getAlt() < 3000)
                .map(FlightTelemetry::assignAirport)
                .window(sliding(60_000, 30_000))
                .groupingKey(Aircraft::getId)
                .aggregate(allOf(
                        toList(),
                        linearTrend(Aircraft::getPosTime, Aircraft::getAlt),
                        FlightTelemetry::assignVerticalDirection
                )); // (timestamp, aircraft_id, aircraft_with_assigned_trend)

        // Enrich aircraft with the noise info and calculate max noise
        // in 60secs windows sliding by 30secs.
        StreamStage<TimestampedEntry<String, Integer>> maxNoise = flights
                .map(e -> entry(e.getValue(), getNoise(e.getValue()))) // (aircraft, noise)
                .window(sliding(60_000, 30_000))
                .groupingKey(e -> e.getKey().getAirport() + "_AVG_NOISE")
                .aggregate(maxBy(comparingInt(Entry::getValue)))
                .map(e -> new TimestampedEntry<>(e.getTimestamp(), e.getKey(), e.getValue().getValue())); // (airport, max_noise)

        // Enrich aircraft with the C02 emission info and calculate total noise
        // in 60secs windows sliding by 30secs.
        StreamStage<TimestampedEntry<String, Double>> co2Emission = flights
                .map(e -> entry(e.getValue(), getCO2Emission(e.getValue()))) // (aircraft, co2_emission)
                .window(sliding(60_000, 30_000))
                .groupingKey(entry -> entry.getKey().getAirport() + "_C02_EMISSION")
                .aggregate(summingDouble(Entry::getValue)); // (airport, total_co2)

        // Drain all results to the Graphite sink
        GeneralStage[] resultStages = {flights, co2Emission, maxNoise};
        pipeline.drainTo(GraphiteSink.build("127.0.0.1", 2004), resultStages);

        return pipeline;
    }

    /**
     * Application entry point.
     * @param args args
     */
    public static void main(String[] args) {
        System.setProperty("hazelcast.logging.type", "log4j");

        // Emulating 2 nodes cluster
        Jet.newJetInstance();
        Jet.newJetInstance();

        Pipeline pipeline = buildPipeline();

        try {
            JetInstance client = Jet.newJetClient();
            Job job = client.newJob(pipeline);
            job.join();
        } finally {
            Jet.shutdownAll();
        }
    }

    /**
     * Finish function of double aggregation which assigns the vertical direction.
     * @param events result of 1st aggregation
     * @param coefficient result of the 2nd aggregation
     * @return The aircraft, enriched with the vertical direction info
     */
    private static Aircraft assignVerticalDirection(List<Aircraft> events, Double coefficient) {
        Aircraft aircraft = events.get(events.size() - 1);
        aircraft.setVerticalDirection(getVerticalDirection(coefficient));
        return aircraft;
    }

    /**
     * Returns the average C02 emission on landing/take-offfor the aircraft
     *
     * @param aircraft
     * @return avg C02 for the aircraft
     */
    private static Double getCO2Emission(Aircraft aircraft) {
        return typeToLTOCycyleC02Emission.getOrDefault(aircraft.getType(), 0d);
    }

    /**
     * Returns the noise level at the current altitude of the aircraft
     *
     * @param aircraft
     * @return noise level of the aircraft
     */
    private static Integer getNoise(Aircraft aircraft) {
        Long altitude = aircraft.getAlt();
        SortedMap<Integer, Integer> lookupTable = getPhaseNoiseLookupTable(aircraft);
        if (lookupTable.isEmpty()) {
            return 0;
        }
        return lookupTable.tailMap(altitude.intValue()).values().iterator().next();
    }

    /**
     * Sets the airport field of the aircraft by looking at the coordinates of it
     *
     * @param aircraft
     */
    private static Aircraft assignAirport(Aircraft aircraft) {
        if (aircraft.getAlt() > 0 && !aircraft.isGnd()) {
            String airport = getAirport(aircraft.getLon(), aircraft.getLat());
            if (airport == null) {
                return null;
            }
            aircraft.setAirport(airport);
        }
        return aircraft;
    }

    /**
     * Returns the vertical direction based on the linear trend coefficient of the altitude
     *
     * @param coefficient
     * @return VerticalDirection enum value
     */
    private static VerticalDirection getVerticalDirection(double coefficient) {
        if (coefficient == Double.NaN) {
            return UNKNOWN;
        }
        if (coefficient > 0) {
            return ASCENDING;
        } else if (coefficient == 0) {
            return CRUISE;
        } else {
            return DESCENDING;
        }
    }
}
