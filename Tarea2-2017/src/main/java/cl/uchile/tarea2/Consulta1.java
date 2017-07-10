package cl.uchile.tarea2;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;


import com.backtype.cascading.tap.PailTap;
import com.backtype.cascading.tap.PailTap.PailTapOptions;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;
import com.twitter.maple.tap.StdoutTap;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.operation.FilterCall;
import cascading.operation.FunctionCall;
import cascading.operation.aggregator.Sum;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;
import cascalog.CascalogFilter;
import cascalog.CascalogFunction;
import jcascalog.Api;
import jcascalog.Option;
import jcascalog.Subquery;
import jcascalog.op.Avg;
import jcascalog.op.Count;
import jcascalog.op.Div;
import jcascalog.op.Limit;
import schema.Data;
import servinglayer.batchview;
import structure.SplitDataPailStructure;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;
import org.json.simple.JSONObject;

/**
 * Batch views
 * Q1: Selects avg amount spent per purchase by each client 
 * @author Paula
 *
 */
public class Consulta1 {
	//Consulta 1: Promedio por boleta de cada cliente
	
	private static String dia = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
	private static FileSystem fs;
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		fs=FileSystem.get(conf);
		System.out.println(fs.getHomeDirectory());
		promedioBoletaCliente();
	}
	
	public static PailTap getDataTap(String path) {
		PailTapOptions opts = new PailTapOptions();
		opts.spec =new PailSpec((PailStructure) new SplitDataPailStructure());
		return new PailTap(path,opts);
	}

	public static class ExtractClienteBoleta extends CascalogFunction{
		public void operate(FlowProcess process, FunctionCall call){
			Data data =((Data)call.getArguments().getObject(0));
			String rut = data.getDataunit().getCliente_boleta().getCliente().getRut();
			String boletaId = data.getDataunit().getCliente_boleta().getBoleta().getBoleta_id();
			call.getOutputCollector().add(new Tuple (rut, boletaId, dia));
		}
	}

	public static class ExtractBoletaTotal extends CascalogFunction {
		public void operate(FlowProcess process, FunctionCall call) {
			Data data =((Data)call.getArguments().getObject(0));
			//long time = data.getPedigree().getTrue_as_of_secs();
			if (data.getDataunit().getBoleta_property().getProperty().isSetTotal()) {
				String boletaId = data.getDataunit().getBoleta_property().getId().getBoleta_id();
				double total = data.getDataunit().getBoleta_property().getProperty().getTotal();
				call.getOutputCollector().add(new Tuple (boletaId, total));
			}
		}
	}
	
	/**
	 * Esta funcion entrega (esperemos) el promedio por boleta de cada cliente
	 */
	public static void promedioBoletaCliente() {
		PailTap clienteBoleta = getDataTap("C:/Paula/masterset/data/9");
		PailTap boletaProperty = getDataTap("C:/Paula/masterset/data/4");
		
		Subquery promedio = new Subquery("?Cliente_id", "?Promedio", "?Dia")
				.predicate(clienteBoleta, "_", "?data")
				.predicate(boletaProperty, "_", "?data2")
				.predicate(new ExtractClienteBoleta(), "?data").out("?Cliente_id", "?Boleta_id", "?Dia")
				.predicate(new ExtractBoletaTotal(), "?data2").out("?Boleta_id","?Total")
				.predicate(new Avg(), "?Total").out("?Promedio");
		
		Subquery tobatchview = new Subquery("?json")
				.predicate(promedio, "?Cliente", "?Promedio", "?Dia")
				.predicate(new ToJSON(), "?Cliente", "?Promedio", "?Dia").out("?json");
		Api.execute(new batchview(), tobatchview);
		//Api.execute(new StdoutTap(), promedio);
	}
	
	/**
	 * Query output to JSON
	 * @author Paula
	 *
	 */
	public static class ToJSON extends CascalogBuffer {
		
		@Override
		public void operate(FlowProcess process, BufferCall call) {
			Iterator<TupleEntry> it = call.getArgumentsIterator();
			
			ObjectMapper mapperAll = new ObjectMapper();
			
	        ArrayList<String> array = new ArrayList<String>();
	        	        
	        while(it.hasNext()) {
	        	TupleEntry val = it.next();
	            ObjectNode obj = mapperAll.createObjectNode();
	        	obj.put("Cliente", val.getString("?Cliente"));
	        	obj.put("Promedio_compras", val.getString("?Promedio"));
	        	obj.put("Dia", val.getString("?Dia"));
	        	array.add(obj.toString());
	        }
	        call.getOutputCollector().add(new Tuple(array));
		}
	}
}
