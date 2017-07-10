** Para cargar / consultar una batchview de una consulta en el serving layer **
- Ejecutar la consulta deseada (Consulta1.java, Consulta2.java o Consulta3.java)
- Esto generará el archivo batchviews.json que usaremos para cargar en la serving layer
- Ejecutar el Druid correspondiente (Druid.java para Consulta1, Druid2.java para Consulta2 o Druid3.java para Consulta3) según las instrucciones (tener corriendo el Druid.sh en master etc)
- Luego de que termine de cargarse, se puede consultar comentando la linea "load_batch()" en el main de Druid.java y descomentando la linea "//query()"