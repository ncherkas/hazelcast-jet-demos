package com.hazelcast.jet.demo.util;

import com.hazelcast.com.eclipsesource.json.JsonValue;
import com.hazelcast.jet.demo.types.Aircraft;
import com.hazelcast.jet.demo.types.WakeTurbulanceCategory;

import java.util.List;
import java.util.SortedMap;
import java.util.stream.Collectors;

import static com.hazelcast.jet.demo.types.Aircraft.VerticalDirection.ASCENDING;
import static com.hazelcast.jet.demo.types.Aircraft.VerticalDirection.DESCENDING;
import static com.hazelcast.jet.demo.Constants.*;
import static com.hazelcast.jet.demo.types.WakeTurbulanceCategory.HEAVY;
import static java.util.Collections.EMPTY_LIST;
import static java.util.Collections.emptySortedMap;

/**
 * Helper methods for JSON parsing and geographic calculations.
 */
public class Util {

    private static float IST_LAT = 40.982555f;
    private static float IST_LON = 28.820829f;

    private static float LHR_LAT = 51.470020f;
    private static float LHR_LON = -0.454295f;

    private static float FRA_LAT = 50.110924f;
    private static float FRA_LON = 8.682127f;

    private static float ATL_LAT = 33.640411f;
    private static float ATL_LON = -84.419853f;

    private static float PAR_LAT = 49.0096906f;
    private static float PAR_LON = 2.54792450f;

    private static float TOK_LAT = 35.765786f;
    private static float TOK_LON = 140.386347f;

    private static float JFK_LAT = 40.6441666667f;
    private static float JFK_LON = -73.7822222222f;


    private static boolean inIstanbul(float lon, float lat) {
        return inBoundariesOf(lon, lat, boundingBox(IST_LON, IST_LAT, 80f));
    }

    private static boolean inLondon(float lon, float lat) {
        return inBoundariesOf(lon, lat, boundingBox(LHR_LON, LHR_LAT, 80f));
    }

    private static boolean inFrankfurt(float lon, float lat) {
        return inBoundariesOf(lon, lat, boundingBox(FRA_LON, FRA_LAT, 80f));
    }

    private static boolean inAtlanta(float lon, float lat) {
        return inBoundariesOf(lon, lat, boundingBox(ATL_LON, ATL_LAT, 80f));
    }

    private static boolean inParis(float lon, float lat) {
        return inBoundariesOf(lon, lat, boundingBox(PAR_LON, PAR_LAT, 80f));
    }

    private static boolean inTokyo(float lon, float lat) {
        return inBoundariesOf(lon, lat, boundingBox(TOK_LON, TOK_LAT, 80f));
    }

    private static boolean inNYC(float lon, float lat) {
        return inBoundariesOf(lon, lat, boundingBox(JFK_LON, JFK_LAT, 80f));
    }

    private static double[] boundingBox(float lon, float lat, float radius) {
        double boundingLon1 = lon + radius / Math.abs(Math.cos(Math.toRadians(lat)) * 69);
        double boundingLon2 = lon - radius / Math.abs(Math.cos(Math.toRadians(lat)) * 69);
        double boundingLat1 = lat + (radius / 69);
        double boundingLat2 = lat - (radius / 69);
        return new double[]{boundingLon1, boundingLat1, boundingLon2, boundingLat2};
    }

    private static boolean inBoundariesOf(float lon, float lat, double[] boundaries) {
        return !(lon > boundaries[0] || lon < boundaries[2]) &&
                !(lat > boundaries[1] || lat < boundaries[3]);
    }


    public static double asDouble(JsonValue value) {
        return value == null ? -1.0 : value.asDouble();
    }

    public static float asFloat(JsonValue value) {
        return value == null ? -1.0f : value.asFloat();
    }

    public static int asInt(JsonValue value) {
        return value == null || !value.isNumber() ? -1 : value.asInt();
    }

    public static long asLong(JsonValue value) {
        return value == null ? -1 : value.asLong();
    }

    public static String asString(JsonValue value) {
        return value == null ? "" : value.asString();
    }

    public static boolean asBoolean(JsonValue value) {
        return value != null && value.asBoolean();
    }

    public static String[] asStringArray(JsonValue value) {
        if (value == null) {
            return new String[]{""};
        } else {
            List<JsonValue> valueList = value.asArray().values();
            List<String> strings = valueList.stream().map(JsonValue::asString).collect(Collectors.toList());
            return strings.toArray(new String[strings.size()]);
        }
    }

    public static List<Double> asDoubleList(JsonValue value) {
        if (value == null) {
            return EMPTY_LIST;
        } else {
            List<JsonValue> valueList = value.asArray().values();
            return valueList.stream().filter(JsonValue::isNumber).map(JsonValue::asDouble).collect(Collectors.toList());
        }
    }

    /**
     * Returns if the aircraft is in 80 mile radius area of the airport.
     *
     * @param lon longitude of the aircraft
     * @param lat latitude of the aircraft
     * @return name of the airport
     */
    public static String getAirport(float lon, float lat) {
        if (inLondon(lon, lat)) {
            return "London";
        } else if (inIstanbul(lon, lat)) {
            return "Istanbul";
        } else if (inFrankfurt(lon, lat)) {
            return "Frankfurt";
        } else if (inAtlanta(lon, lat)) {
            return "Atlanta";
        } else if (inParis(lon, lat)) {
            return "Paris";
        } else if (inTokyo(lon, lat)) {
            return "Tokyo";
        } else if (inNYC(lon, lat)) {
            return "New York";
        }
        // unknown city
        return null;
    }

    /**
     * Returns altitude to noise level lookup table for the aircraft based on its weight category
     *
     * @param aircraft
     * @return SortedMap contains altitude to noise level mappings.
     */
    public static SortedMap<Integer, Integer> getPhaseNoiseLookupTable(Aircraft aircraft) {
        Aircraft.VerticalDirection verticalDirection = aircraft.getVerticalDirection();
        WakeTurbulanceCategory wtc = aircraft.getWtc();
        if (ASCENDING.equals(verticalDirection)) {
            if (HEAVY.equals(wtc)) {
                return heavyWTCClimbingAltitudeToNoiseDb;
            } else {
                return mediumWTCClimbingAltitudeToNoiseDb;
            }
        } else if (DESCENDING.equals(verticalDirection)) {
            if (HEAVY.equals(wtc)) {
                return heavyWTCDescendAltitudeToNoiseDb;
            } else {
                return mediumWTCDescendAltitudeToNoiseDb;
            }
        }
        return emptySortedMap();
    }
}
