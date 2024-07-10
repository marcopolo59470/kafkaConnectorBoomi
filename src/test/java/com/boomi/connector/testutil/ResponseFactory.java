
package com.boomi.connector.testutil;

public class ResponseFactory {

    public static SimpleOperationResponse get(SimpleTrackedData... objects) {
        SimpleOperationResponse response = new SimpleOperationResponse();
        for (SimpleTrackedData o : objects) {
            response.addTrackedData(o);
        }
        return response;
    }
}
