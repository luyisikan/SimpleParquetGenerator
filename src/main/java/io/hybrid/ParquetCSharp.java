package io.hybrid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.NanoTime;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Random;

public class ParquetCSharp {
    public static void main(String[] args) throws IOException {
        System.setProperty("hadoop.home.dir", "d:\\dev\\winutils\\hadoop-2.6.3");

        String targetFile = "d:\\data\\parquet-full-type.parquet";
        java.nio.file.Path path = java.nio.file.Paths.get(targetFile);

        if (Files.exists(path)) {
            Files.delete(path);
        }

        Random rand = new Random();
        String schemaStr = "message schema {" +

                // boolean primitive
                "optional BOOLEAN BOOLEAN_;\n" +

                // int 32 primitive
                "optional INT32 INT32_;\n" +
                "optional INT32 INT32_INT8 (INT_8);\n" +
                "optional INT32 INT32_INT16 (INT_16);\n" +
                "optional INT32 INT32_INT32 (INT_32);\n" +
                "optional INT32 INT32_UINT8 (UINT_8);\n" +
                "optional INT32 INT32_UINT16 (UINT_16);\n" +
                "optional INT32 INT32_UINT32 (UINT_32);\n" +
                "optional INT32 INT32_DATE (DATE);\n" +
                "optional INT32 INT32_TIME_MILLIS (TIME_MILLIS);\n" +
                "optional INT32 INT32_DECIMAL_5_4 (DECIMAL(5,4));\n" +

                // int 64 primitive
                "optional INT64 INT64_;\n" +
                "optional INT64 INT64_INT64 (INT_64);\n" +
                "optional INT64 INT64_UINT64 (UINT_64);\n" +
                "optional INT64 INT64_TIMESTAMP_MILLIS (TIMESTAMP_MILLIS);\n" +
                "optional INT64 INT64_DECIMAL_11_5 (DECIMAL(11,5));\n" +

                // int 96 primitive
                "optional INT96 INT96_;\n" +

                // float primitive
                "optional FLOAT FLOAT_;\n" +

                // double primitive
                "optional DOUBLE DOUBLE_;\n" +

                // binary primitive
                "optional BINARY BINARY_;\n" +
                "optional BINARY BINARY_UTF8 (UTF8);\n" +
                "optional BINARY BINARY_ENUM (ENUM);\n" +
                "optional BINARY BINARY_JSON (JSON);\n" +
                "optional BINARY BINARY_BSON (BSON);\n" +
                "optional BINARY BINARY_DECIMAL_19_2 (DECIMAL(19,2));\n" +

                // fixed length byte array primitive
                "optional FIXED_LEN_BYTE_ARRAY(5) FIXED_;\n" +
                //"optional FIXED_LEN_BYTE_ARRAY(12) FIXED_INTERVAL (INTERVAL);\n" +  // length must be 12
                "optional FIXED_LEN_BYTE_ARRAY(9) FIXED_DECIMAL_19_2 (DECIMAL(19, 2));\n" +
                "}";

        MessageType schema = MessageTypeParser.parseMessageType(schemaStr);

        ExampleParquetWriter.Builder builder = ExampleParquetWriter
                .builder(new Path(targetFile))
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withConf(new Configuration())
                .withType(schema);

        ParquetWriter<Group> writer = builder.build();
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);

        Group group = groupFactory.newGroup()
                // boolean
                .append("BOOLEAN_", rand.nextBoolean())
                // int 32
                .append("INT32_", rand.nextInt(2147483647))
                .append("INT32_INT8", rand.nextInt(127))
                .append("INT32_INT16", rand.nextInt(32767))
                .append("INT32_INT32", rand.nextInt(2147483647))
                .append("INT32_UINT8", rand.nextInt(127))
                .append("INT32_UINT16", rand.nextInt(32767))
                .append("INT32_UINT32", rand.nextInt(2147483647))
                .append("INT32_DATE", rand.nextInt(2147483647))
                .append("INT32_TIME_MILLIS", rand.nextInt(2147483647))
                .append("INT32_DECIMAL_5_4", rand.nextInt(2147483647))
                // int 64
                .append("INT64_", rand.nextLong())
                .append("INT64_INT64", rand.nextLong())
                .append("INT64_UINT64", rand.nextLong())
                .append("INT64_TIMESTAMP_MILLIS", rand.nextLong())
                .append("INT64_DECIMAL_11_5", rand.nextLong())
                // int 96
                .append("INT96_", new NanoTime(2440589, 0))
                // float
                .append("FLOAT_", rand.nextFloat())
                // double
                .append("DOUBLE_", rand.nextDouble())
                // binary
                .append("BINARY_", Binary.fromConstantByteArray(new byte[]{(byte) rand.nextInt(127), (byte) rand.nextInt(127), (byte) rand.nextInt(127), (byte) rand.nextInt(127), (byte) rand.nextInt(127)}))
                .append("BINARY_UTF8", Binary.fromString(GetRandomString()))
                .append("BINARY_ENUM", Binary.fromString("ENUM_VALUE"))
                .append("BINARY_JSON", Binary.fromString("{\"value\": [1, 2, 3, 4, 5] }"))
                .append("BINARY_BSON", Binary.fromConstantByteArray(   // { hello, world }
                        new byte[]{0x16, 0x00, 0x00, 0x00, 0x02, 0x00, 0x06, 0x00, 0x00, 0x00, 0x00, 0x00}))
                .append("BINARY_DECIMAL_19_2", Binary.fromConstantByteArray(    // -12398732340542134.03
                        new byte[] { (byte)255, (byte)238, (byte)203, 21, (byte)220, (byte)221, (byte)175, 16, (byte)229 }))
                // fix
                .append("FIXED_", Binary.fromConstantByteArray(new byte[]{1, 2, 3, 4, 5}))
                //.append("FIXED_INTERVAL", Binary.fromConstantByteArray(new byte[]{1, 2, 3, 4, 5}))
                .append("FIXED_DECIMAL_19_2", Binary.fromConstantByteArray( // 12398732340542134.03
                        new byte[] { 0, 17, 52, (byte)234, 35, 34, 80, (byte)239, 27 }))
                ;

        writer.write(group);
        writer.close();
    }
    public static String GetRandomString() {
        byte[] array = new byte[7]; // length is bounded by 7
        new Random().nextBytes(array);
        return new String(array, Charset.forName("UTF-8"));
    }

}

