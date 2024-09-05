package com.boomi.connector.kafka.util;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.apache.avro.io.Decoder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.stream.Collectors;

public class AvroMapper {
    private final Schema.Parser parser = new Schema.Parser();
    private final DecoderFactory decoderFactory = new DecoderFactory();
    private final Schema schema;
    private final DatumReader<GenericData.Record> reader;


    /**public AvroMapper(@NonNull String schema) {
        this.schema = toAvroSchema(schema);
        this.reader = new GenericDatumReader<>(this.schema);
    }*/

    public AvroMapper(String schemaString) {
        this.schema = new Schema.Parser().parse(schemaString);
        this.reader = new GenericDatumReader<>(schema);
    }

    public Schema toAvroSchema(@NonNull String avroSchema) {
        return parser.parse(avroSchema);
    }

    public GenericData.Record _toAvroRecord(@NonNull String data) throws IOException {
        var decoder = decoderFactory.jsonDecoder(schema, data);
        return reader.read(null, decoder);
    }

    public GenericData.Record toAvroRecord(@NonNull String data) throws IOException {
        try {
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, data);
            return reader.read(null, decoder);
        } catch (IOException e) {
            System.err.println("Erreur lors de la conversion JSON vers Avro Record : " + e.getMessage());
            throw e;
        }
    }

    public static String convertInputStreamToString(InputStream inputStream) throws IOException {
        try (BufferedReader buffer = new BufferedReader(new InputStreamReader(inputStream))) {
            return buffer.lines().collect(Collectors.joining("\n"));
        }
    }

    // Getters for schema and reader if needed
    public Schema getSchema() {
        return schema;
    }

    public DatumReader<GenericData.Record> getReader() {
        return reader;
    }
}
