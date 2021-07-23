package com.app.spark.module;
import com.fasterxml.jackson.annotation.JsonFormat;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.util.Date;

@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class SignalPoint {
    private String gatewayID;
    private String uplinkID;
    private String latitude;
    private String longitude;
    private int spreadingFactor;
    private String codeRate;
    private String _latitude;
    private String _longitude;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss", timezone="IST")
    private Date timestamp;
    private int _rssi;
    private int _snr;

}
