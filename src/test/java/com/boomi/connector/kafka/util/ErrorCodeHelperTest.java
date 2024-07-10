
package com.boomi.connector.kafka.util;

import org.apache.kafka.common.errors.RecordTooLargeException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Assert;
import org.junit.experimental.theories.DataPoints;
import org.junit.experimental.theories.Theories;
import org.junit.experimental.theories.Theory;
import org.junit.runner.RunWith;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

@RunWith(Theories.class)
public class ErrorCodeHelperTest {

    @DataPoints
    public static Collection<ExceptionAndErrorCodeVO> parameters() {
        Collection<ExceptionAndErrorCodeVO> parameters = new ArrayList<>(3);

        // Timeout
        ExecutionException timeoutException = Mockito.mock(ExecutionException.class);
        Mockito.when(timeoutException.getCause()).thenReturn(new TimeoutException());
        parameters.add(new ExceptionAndErrorCodeVO(timeoutException, Constants.CODE_TIMEOUT));

        // Invalid Size
        ExecutionException invalidSizeException = Mockito.mock(ExecutionException.class);
        Mockito.when(invalidSizeException.getCause()).thenReturn(new RecordTooLargeException());
        parameters.add(new ExceptionAndErrorCodeVO(invalidSizeException, Constants.CODE_INVALID_SIZE));

        // Generic Error
        ExecutionException genericException = Mockito.mock(ExecutionException.class);
        Mockito.when(genericException.getCause()).thenReturn(new IOException());
        parameters.add(new ExceptionAndErrorCodeVO(genericException, Constants.CODE_ERROR));

        return parameters;
    }

    @Theory
    public void getErrorCodeTest(ExceptionAndErrorCodeVO exceptionAndErrorCodeVO) {
        String errorCode = ErrorCodeHelper.getErrorCode(exceptionAndErrorCodeVO._exception);
        Assert.assertEquals(exceptionAndErrorCodeVO._errorCode, errorCode);
    }

    private static class ExceptionAndErrorCodeVO {

        private final ExecutionException _exception;
        private final String _errorCode;

        ExceptionAndErrorCodeVO(ExecutionException exception, String errorCode) {
            _exception = exception;
            _errorCode = errorCode;
        }
    }
}
