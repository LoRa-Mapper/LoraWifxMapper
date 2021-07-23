package com.app.spark.utils;

import com.app.spark.module.SignalPoint;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class Deserializer implements Decoder<SignalPoint> {
    private static ObjectMapper objectMapper = new ObjectMapper();
  public Deserializer(VerifiableProperties vp){

  }
    @Override
    public SignalPoint fromBytes(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, SignalPoint.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
