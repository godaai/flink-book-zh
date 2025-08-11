package com.flink.tutorials.java.utils.stock;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.src.reader.SimpleStreamFormat;
import org.apache.flink.core.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class MediaReaderFormat extends SimpleStreamFormat<Media> {

    @Override
    public Reader<Media> createReader(Configuration config, FSDataInputStream stream) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(stream));
        return new Reader<Media>() {
            @Override
            public Media read() throws IOException {
                String line = reader.readLine();
                if (line == null) {
                    return null;
                }

                String[] parts = line.split(",");
                if (parts.length != 3) {
                    throw new IOException("Invalid input format: " + line);
                }

                String symbol = parts[0];
                long timestamp = Long.parseLong(parts[1]);
                String status = parts[2];

                return new Media(symbol, timestamp, status);
            }

            @Override
            public void close() throws IOException {
                reader.close();
            }
        };
    }

    @Override
    public TypeInformation<Media> getProducedType() {
        return TypeInformation.of(Media.class);
    }
}