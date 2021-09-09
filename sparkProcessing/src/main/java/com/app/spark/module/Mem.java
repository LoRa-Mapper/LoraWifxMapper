package com.app.spark.module;

import com.influxdb.annotations.Column;
import com.influxdb.annotations.Measurement;

import java.time.Instant;

@Measurement(name = "mem")
public class Mem {
    @Column
    String id;
    @Column
    String gateway_id;
    @Column
    double dataRate;
    @Column
    double frequency;
    @Column
    int rssi;
    @Column
    int snr;
    @Column
    double latitude;
    @Column
    double longitude;
    @Column(timestamp = true)
    Instant time;
}