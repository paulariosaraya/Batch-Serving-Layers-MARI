/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package servinglayer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 *
 * @author Pipe
 */
public class Druid {
	//Para Consulta1

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    public static void main(String[] args) throws IOException, Exception {

        load_batch();
        //query();
    }

    public static void load_batch() throws IOException {
        MediaType JSON = MediaType.parse("application/json");
        OkHttpClient client = new OkHttpClient();

        String url = "http://192.168.233.128:8090/druid/indexer/v1/task";
        byte[] array = Files.readAllBytes(Paths.get("retail-index.json"));
        //byte[] array = json.getBytes();

        RequestBody body = RequestBody.create(JSON, array);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        System.out.println(request.body().contentType());
        Response response = client.newCall(request).execute();
        System.out.println(response.code());
        System.out.println(response.body().string());
    }
    
    public static void query() throws IOException {
        MediaType JSON = MediaType.parse("application/json");

        OkHttpClient client = new OkHttpClient();

        String url = "http://192.168.233.128:8083/druid/v2";

        //mapper de jackson
        ObjectMapper mapper_all = new ObjectMapper();
        //creamos nodo objeto
        ObjectNode query = mapper_all.createObjectNode();
        ObjectNode metrica_compras = mapper_all.createObjectNode();

        //creamos un nodo array
        ArrayNode dimensiones = mapper_all.createArrayNode();
        ArrayNode agregaciones = mapper_all.createArrayNode();

        query.put("queryType", "groupBy");
        query.put("dataSource", "numboletas");
        query.put("intervals", "2012-01-01T00:00:00.000/2018-01-03T00:00:00.000");
        query.put("granularity", "day");
        //dimensiones
        dimensiones.add("Cliente");
        //dimensiones.add("Dia");
        query.put("dimensions", dimensiones);

        //agregaciones
        metrica_compras.put("type", "doubleSum");
        metrica_compras.put("name", "Promedio_compras");
        metrica_compras.put("fieldName", "Promedio_compras");
        agregaciones.add(metrica_compras);
        query.put("aggregations", agregaciones);

        String json = query.toString();
        System.out.println(json);
        byte[] array = json.getBytes();

        RequestBody body = RequestBody.create(JSON, array);
        Request request = new Request.Builder()
                .url(url)
                .post(body)
                .build();
        System.out.println(request.body().contentType());
        Response response = client.newCall(request).execute();
        System.out.println(response.code());
        System.out.println(response.body().string());
    }

}
