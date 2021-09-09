package com.app.spark.utils;

import com.app.spark.module.Mem;
import com.fasterxml.jackson.databind.ObjectMapper;
import kafka.serializer.Decoder;
import kafka.utils.VerifiableProperties;

public class Deserializer implements Decoder<Mem> {
    private static ObjectMapper objectMapper = new ObjectMapper();
  public Deserializer(VerifiableProperties vp){

  }
    @Override
    public Mem fromBytes(byte[] bytes) {
        try {
            return objectMapper.readValue(bytes, Mem.class);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }
}
