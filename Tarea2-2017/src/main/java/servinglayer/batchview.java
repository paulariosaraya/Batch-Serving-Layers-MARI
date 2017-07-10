package servinglayer;

import cascading.flow.hadoop.HadoopFlowProcess;
import cascading.scheme.hadoop.SequenceFile;
import cascading.tap.hadoop.Lfs;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntryIterator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;

public class batchview extends Lfs {

    public batchview() {
        super(new SequenceFile(Fields.ALL), getTempDir());
    }

    public static String getTempDir() {
        final File temp;
        try {
            temp = File.createTempFile("temp", Long.toString(System.nanoTime()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        temp.deleteOnExit();
        if (!(temp.delete())) {
            throw new RuntimeException("Could not delete temp file: " + temp.getAbsolutePath());
        }

        return temp.getAbsoluteFile().getPath();
    }

    @Override
    public boolean commitResource(JobConf conf) throws java.io.IOException {
//por batch.. cada linea es un json
        try (TupleEntryIterator it = new HadoopFlowProcess(conf).openTapForRead(this)) {
            System.out.println("");
            System.out.println("");
            System.out.println("RESULTS");
            System.out.println("-----------------------");
            while (it.hasNext()) {
                //Aqui capturamos cada registro
                System.out.println(it.next().getTuple());

                // como lo pasamos a un array, ahora podemos iterar sobre eso
                JsonNode results = new ObjectMapper().readTree(it.next().getTuple().toString());
                if (results.isArray()) {
                	//hacer cambios sobre batchviews.json
                    try (PrintWriter writer = new PrintWriter("batchviews.json", "UTF-8")) {
                        for (JsonNode row : results) {
                            writer.println(row.toString());
                        }
                    }
                }

            }
            System.out.println("-----------------------");
        }

        return true;
    }
}
