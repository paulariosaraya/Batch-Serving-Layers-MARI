# Batch-Serving-Layers-MARI
Tarea 2 Big Data

Grupo 7 (Gabriel Dintrans, Paula Ríos, Carlos Vega)

### Descripción
Se tienen datos de boletas del retail MARI, los cuales se almacenan a una cola local en Kafka. Para esto se realiza lo siguiente (código entregado por el equipo docente):
- Definición de un Graph Schema y estructura en Apache Thrift.
- Generar _schema_ para Java.
- Almacenamiento de los datos en el HDFS de un cluster Hadoop, usando Pail y serializándolos con Apache Thrift.

Además se responden las siguientes consultas:
- ¿Existe alguna categoría que venda más?
- ¿Cuál es el promedio por boleta de cada cliente?
- ¿Cuales son los 10 productos más vendidos?
- ¿En qué sucursal se hacen más ventas?
- Y como bonus: ubicación (longitud y latitud) de cada sucursal.

Para lo cual se realizan las onsultas pedidas usando jCascalog las cuales luego son almacenadas en una _Serving Layer_ y pueden ser consultadas mediante Druid.

### Para cargar / consultar una batchview de una consulta en el serving layer
- Ejecutar la consulta deseada (Consulta1.java, Consulta2.java o Consulta3.java)
- Esto generará el archivo batchviews.json que usaremos para cargar en la serving layer
- Ejecutar el Druid correspondiente (Druid.java para Consulta1, Druid2.java para Consulta2 o Druid3.java para Consulta3) según las instrucciones (tener corriendo el Druid.sh en master etc)
- Luego de que termine de cargarse, se puede consultar comentando la linea "load_batch()" en el main de Druid.java y descomentando la linea "//query()"
