package cl.uchile.tarea2;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import com.backtype.cascading.tap.PailTap;
import com.backtype.cascading.tap.PailTap.PailTapOptions;
import com.backtype.hadoop.pail.PailSpec;
import com.backtype.hadoop.pail.PailStructure;
import com.twitter.maple.tap.StdoutTap;

import cascading.flow.FlowProcess;
import cascading.operation.BufferCall;
import cascading.operation.FilterCall;
import cascading.operation.FunctionCall;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascalog.CascalogBuffer;
import cascalog.CascalogFilter;
import cascalog.CascalogFunction;
import jcascalog.Api;
import jcascalog.Option;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.Limit;
import schema.Data;
import servinglayer.batchview;
import structure.SplitDataPailStructure;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Q2: Selects top 10 most purchased products
 * @author Paula
 *
 */
public class Consulta2 {

	private static String dia = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
	private static FileSystem fs;
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		fs=FileSystem.get(conf);
		System.out.println(fs.getHomeDirectory());
		top10Productos();
	}
	
	public static PailTap getDataTap(String path) {
		PailTapOptions opts = new PailTapOptions();
		opts.spec =new PailSpec((PailStructure) new SplitDataPailStructure());
		return new PailTap(path,opts);
	}

	public static class ExtractBoletaProducto extends CascalogFunction{
		public void operate(FlowProcess process, FunctionCall call){
			Data data =((Data)call.getArguments().getObject(0));
			String boletaId = data.getDataunit().getBoleta_producto().getBoleta().getBoleta_id();
			int productoId = data.getDataunit().getBoleta_producto().getProducto().getItemid();
			call.getOutputCollector().add(new Tuple (boletaId, productoId, dia));
		}
	}

	//esta funcion entrega (esperemos) los 10 productos con mas ventas
	public static void top10Productos() {
		PailTap BoletaProducto = getDataTap("C:/Paula/masterset/data/19");
		
		Subquery contar = new Subquery("?Producto_id", "?Count", "?dia")
				.predicate(BoletaProducto, "_", "?data")
				.predicate(new ExtractBoletaProducto(), "?data").out("?Boleta_id", "?Producto_id", "?dia")
				.predicate(new Count(), "?Count");
		
		Subquery top10 = new Subquery("?Producto", "?Ventas", "?Dia")
				.predicate(contar, "?Producto_id", "?Count", "?dia")
				.predicate(Option.SORT, "?Count")
				.predicate(Option.REVERSE, "?Count")
				.predicate(new Limit(10), "?Producto_id", "?Count", "?dia").out("?Producto", "?Ventas", "?Dia");
		
		Subquery tobatchview = new Subquery("?json")
				.predicate(top10, "?Producto", "?Ventas", "?Dia")
				.predicate(new ToJSON(), "?Producto", "?Ventas", "?Dia").out("?json");
		Api.execute(new batchview(), tobatchview);
		//Api.execute(new StdoutTap(), top10);
	}
	
	public static class ToJSON extends CascalogBuffer {
		
		@Override
		public void operate(FlowProcess process, BufferCall call) {
			Iterator<TupleEntry> it = call.getArgumentsIterator();
			
			ObjectMapper mapperAll = new ObjectMapper();
			
	        ArrayList<String> array = new ArrayList<String>();
	        	        
	        while(it.hasNext()) {
	        	TupleEntry val = it.next();
	            ObjectNode obj = mapperAll.createObjectNode();
	        	obj.put("Producto", val.getString("?Producto"));
	        	obj.put("Ventas", val.getString("?Ventas"));
	        	obj.put("Dia", val.getString("?Dia"));
	        	array.add(obj.toString());
	        }
	        call.getOutputCollector().add(new Tuple(array));
		}
	}
}
