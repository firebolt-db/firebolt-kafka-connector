package com.firebolt.kafka.connect.ingestion.sql.preparedstatement.datatype.converter;

public class FireboltByteaConverter {

    /**
     * byte[] array = new byte[0] is represented in firebolt by \x
     * @param array
     * @return
     */
    public static byte[] convertFireboltBytea(byte[] array) {
        if (array.length == 0) {
            // In firebolt and empty byte is represented by \x
            array = "\\x".getBytes();
        }
        return array;
    }
}
