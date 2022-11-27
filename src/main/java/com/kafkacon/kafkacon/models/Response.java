package com.kafkacon.kafkacon.models;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder
public class Response<T> {
    private boolean error;
    private ErrorDetails errorDetails;
    private T data;
}
