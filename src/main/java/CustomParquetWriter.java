 // Add your package here 
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

public class CustomParquetWriter extends ParquetWriter<List<String>> {
     public CustomParquetWriter(
            Path file,
            MessageType schema,
            boolean enableDictionary,
            CompressionCodecName codecName
    ) throws IOException {
        super(file, new CustomWriteSupport(schema), codecName, DEFAULT_BLOCK_SIZE, DEFAULT_PAGE_SIZE, enableDictionary, false);
    }

    public static MessageType getSchemaForParquetFile(File schemaFile) throws IOException {
        String rawSchema = new String(Files.readAllBytes(schemaFile.toPath()));
        return MessageTypeParser.parseMessageType(rawSchema);
    }

    public static List<List<String>> getDataForFile() {
        List<List<String>> data = new ArrayList<>();

        List<String> parquetFileItem1 = new ArrayList<>();
        parquetFileItem1.add("1");
        parquetFileItem1.add("Name1");
        parquetFileItem1.add("true");

        List<String> parquetFileItem2 = new ArrayList<>();
        parquetFileItem2.add("2");
        parquetFileItem2.add("Name2");
        parquetFileItem2.add("false");

        data.add(parquetFileItem1);
        data.add(parquetFileItem2);

        return data;
    }

    public static CustomParquetWriter getParquetWriter(MessageType schema, String outputDirectoryPath) throws IOException {
        String outputFilePath = outputDirectoryPath+ "/" + System.currentTimeMillis() + ".parquet";
        File outputParquetFile = new File(outputFilePath);
        Path path = new Path(outputParquetFile.toURI().toString());
        return new CustomParquetWriter(
                path, schema, false, CompressionCodecName.UNCOMPRESSED
        );
    }

    public static void main(String[] args) throws IOException {
        String schemaFilePath = "./src/main/resources/user.schema";
        String outputDirectoryPath = "./src/main/resources/";
        List<List<String>> columns = getDataForFile();


        File schemaFile = new File(schemaFilePath);
        MessageType schema = getSchemaForParquetFile(schemaFile);


        CustomParquetWriter writer = getParquetWriter(schema, outputDirectoryPath);
        for (List<String> column : columns) {
            writer.write(column);
        }
        writer.close();

    }
}

