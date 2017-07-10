/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package cl.uchile.tarea2;

import com.backtype.hadoop.pail.Pail;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.MalformedURLException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import schema.BoletaID;
import schema.BoletaProducto;
import schema.BoletaProperty;
import schema.BoletaPropertyValue;
import schema.ClienteBoleta;
import schema.ClienteID;
import schema.Data;
import schema.DataUnit;
import schema.EmpleadoBoleta;
import schema.EmpleadoID;
import schema.Pedigree;
import schema.ProductoID;
import schema.SucursalBoleta;
import schema.SucursalID;
import schema.TimeStamp;
import structure.SplitDataPailStructure;

/**
 *
 * @author Pipe
 */
public class GetBoletas {

    private static final boolean stop = false;

    public static void main(String[] argv) throws Exception {

        String topicName = "RetailMari";
        String groupId = "yo";

        ConsumerThread consumerRunnable = new ConsumerThread(topicName, groupId);
        consumerRunnable.start();

        consumerRunnable.getKafkaConsumer().wakeup();
        System.out.println("Stopping consumer .....");
        consumerRunnable.join();
    }

    private static class ConsumerThread extends Thread {

        private final String topicName;
        private final String groupId;
        private KafkaConsumer<String, String> kafkaConsumer;

        public ConsumerThread(String topicName, String groupId) {
            this.topicName = topicName;
            this.groupId = groupId;
        }

        @Override
        public void run() {
            Properties configProperties = new Properties();
            configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "50.116.6.150:9092");
            configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
            configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
            kafkaConsumer = new KafkaConsumer<>(configProperties);
            kafkaConsumer.subscribe(Arrays.asList(topicName));

            try {
                Pail<Data> pail = Pail.create("C:/Paula/masterset/data", new SplitDataPailStructure(), false);
                Pail.TypedRecordOutputStream out = pail.openWrite();
                int contador = 0;
                while (true) {
                    ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
                    for (ConsumerRecord<String, String> record : records) {
                        //obtenemos el registro de la cola
                        String json = record.value();

                        //mapeamos el objeto
                        JsonNode object = new ObjectMapper().readTree(json);

                        //obtenemos fecha como string
                        String fecha = object.get("fecha").textValue();

                        // la id de la boleta
                        String boleta_id = object.get("numboleta").textValue();
                        System.out.println(boleta_id);

                        //obtenemos los productos
                        JsonNode productos = object.get("productos");

                        //obtenemos rut cliente
                        String rut_cliente = object.get("cliente").toString();
                        if ("".equals(rut_cliente)) {
                            rut_cliente = null;
                        }
                        System.out.println(rut_cliente);

                        //obtenemos rut empleado
                        String rut_empleado = object.get("empleado").get("rut").toString();

                        //obtenemos id de sucursal
                        String place_id = object.get("empleado").get("sucursal").get("place_id").toString();

                        //iniciamos el total de la boleta
                        double total = 0;

                        if (productos.isArray()) {
                            //itereamos sobre los productos de la boleta
                            for (JsonNode producto : productos) {
                                try {
                                    //System.out.println(producto.get("salePrice").asDouble());
                                    out.writeObject(crearBoletaProducto(boleta_id, producto.get("itemId").asInt(), Long.parseLong(new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime()))));
                                    total = total + producto.get("salePrice").asDouble();
                                } catch (Exception e) {
                                    System.out.println(e);
                                }
                            }

                            out.writeObject(crearBoletaPropertyTimestamp(boleta_id, fecha, Long.parseLong(new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime()))));
                            out.writeObject(crearBoletaPropertyTotal(boleta_id, total, Long.parseLong(new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime()))));
                            System.out.println(boleta_id);
                            out.writeObject(crearClienteBoleta(rut_cliente, boleta_id, Long.parseLong(new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime()))));
                            out.writeObject(crearEmpleadoBoleta(rut_empleado, boleta_id, Long.parseLong(new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime()))));
                            out.writeObject(crearSucursalBoleta(place_id, boleta_id, Long.parseLong(new SimpleDateFormat("yyyyMMddHHmmss").format(Calendar.getInstance().getTime()))));

                        }
                        contador++;
                        if (contador == 100) {
                            contador = 0;
                            out.close();
                            //break;
                            out = pail.openWrite();

                        }
                        System.out.println(contador);
                    }
                }
            } catch (WakeupException ex) {
                System.out.println("Exception caught " + ex.getMessage());
            } catch (MalformedURLException ex) {
                Logger.getLogger(GetBoletas.class.getName()).log(Level.SEVERE, null, ex);
            } catch (IOException ex) {
                Logger.getLogger(GetBoletas.class.getName()).log(Level.SEVERE, null, ex);
            } finally {
                kafkaConsumer.close();
                System.out.println("After closing KafkaConsumer");
            }
        }

        public KafkaConsumer<String, String> getKafkaConsumer() {
            return this.kafkaConsumer;
        }
    }

    public static Data crearBoletaPropertyTimestamp(String boleta_id, String fecha_boleta, long time) {
        DateTimeFormatter a = DateTimeFormatter.ofPattern("yyyy/M/d H:m:s");
        LocalDateTime b = LocalDateTime.from(a.parse(fecha_boleta));

        TimeStamp timestamp = new TimeStamp();
        timestamp.setSecond((byte) b.getSecond());
        timestamp.setMinute((byte) b.getMinute());
        timestamp.setHour((byte) b.getHour());
        timestamp.setDay((byte) b.getDayOfMonth());
        timestamp.setMonth((byte) b.getMonthValue());
        timestamp.setYear((short) b.getYear());

        BoletaPropertyValue c = new BoletaPropertyValue();
        c.setTimestamp(timestamp);

        return new Data(
                new Pedigree(time),
                DataUnit.boleta_property(
                        new BoletaProperty(
                                BoletaID.boleta_id(boleta_id),
                                c
                        )
                )
        );
    }

    public static Data crearBoletaPropertyTotal(String boleta_id, double total, long time) {

        BoletaPropertyValue c = new BoletaPropertyValue();
        c.setTotal(total);

        return new Data(
                new Pedigree(time),
                DataUnit.boleta_property(
                        new BoletaProperty(
                                BoletaID.boleta_id(boleta_id),
                                c
                        )
                )
        );
    }

    public static Data crearBoletaProducto(String boleta_id, int item_id, long time) {
        return new Data(
                new Pedigree(time),
                DataUnit.boleta_producto(
                        new BoletaProducto(
                                BoletaID.boleta_id(boleta_id),
                                ProductoID.itemid(item_id),
                                new Random().nextLong()
                        )
                )
        );
    }

    public static Data crearClienteBoleta(String rut, String boleta_id, long time) {

        return new Data(
                new Pedigree(time),
                DataUnit.cliente_boleta(
                        new ClienteBoleta(
                                ClienteID.rut(rut),
                                BoletaID.boleta_id(boleta_id)
                        )
                )
        );
    }

    public static Data crearEmpleadoBoleta(String rut, String boleta_id, long time) {

        return new Data(
                new Pedigree(time),
                DataUnit.empleado_boleta(
                        new EmpleadoBoleta(
                                EmpleadoID.rut(rut),
                                BoletaID.boleta_id(boleta_id)
                        )
                )
        );
    }

    public static Data crearSucursalBoleta(String place_id, String boleta_id, long time) {

        return new Data(
                new Pedigree(time),
                DataUnit.sucursal_boleta(
                        new SucursalBoleta(
                                SucursalID.place_id(place_id),
                                BoletaID.boleta_id(boleta_id)
                        )
                )
        );
    }
}
