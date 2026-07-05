package com.kd.teams.scaler.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FileProcessEvent implements Serializable {

    private String fileName;

    private String status;

}
