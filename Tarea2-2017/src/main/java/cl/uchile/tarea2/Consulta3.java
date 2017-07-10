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
import jcascalog.op.Div;
import jcascalog.op.Limit;
import jcascalog.op.Sum;
import schema.Data;
import servinglayer.batchview;
import structure.SplitDataPailStructure;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ArrayNode;
import org.codehaus.jackson.node.ObjectNode;

/**
 * Q3: Selects sucursal with the most sales
 * @author Paula
 *
 */
public class Consulta3 {
	
	private static String dia = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
	private static FileSystem fs;
	
	public static void main(String[] args) throws Exception{
		Configuration conf = new Configuration();
		fs=FileSystem.get(conf);
		System.out.println(fs.getHomeDirectory());
		topSucursalVentas();
	}
	
	public static PailTap getDataTap(String path) {
		PailTapOptions opts = new PailTapOptions();
		opts.spec = new PailSpec((PailStructure) new SplitDataPailStructure());
		return new PailTap(path,opts);
	}

	public static class ExtractSucursalBoleta extends CascalogFunction{
		public void operate(FlowProcess process, FunctionCall call){
			Data data =((Data)call.getArguments().getObject(0));
			//long time = data.getPedigree().getTrue_as_of_secs();
			String sucursalId = data.getDataunit().getSucursal_boleta().getSucursal().getPlace_id();
			String boletaId = data.getDataunit().getSucursal_boleta().getBoleta().getBoleta_id();
			call.getOutputCollector().add(new Tuple (sucursalId, boletaId, dia));
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
	
	//esta funcion entrega (esperemos) la sucursal con mas ventas
	public static void topSucursalVentas() {
		PailTap SucursalBoleta = getDataTap("C:/Paula/masterset/data/16");
		PailTap BoletaProperty = getDataTap("C:/Paula/masterset/data/4");
		
		Subquery ventas = new Subquery("?Sucursal_id", "?ventas", "?dia")
				.predicate(SucursalBoleta, "_", "?data")
				.predicate(BoletaProperty, "_", "?data2")
				.predicate(new ExtractSucursalBoleta(), "?data").out("?Sucursal_id", "?Boleta_id", "?dia")
				.predicate(new ExtractBoletaTotal(), "?data2").out("?Boleta_id","?Total")
				.predicate(new Sum(), "?Total").out("?ventas");
		
		Subquery topSucursal = new Subquery("?Sucursal", "?Ventas", "?Dia")
				.predicate(ventas, "?Sucursal_id", "?ventas", "?dia")
				.predicate(Option.SORT, "?ventas")
				.predicate(Option.REVERSE , "?ventas")
				.predicate(new Limit(1), "?Sucursal_id", "?ventas", "?dia").out("?Sucursal", "?Ventas", "?Dia");
		
		Subquery tobatchview = new Subquery("?json")
				.predicate(topSucursal, "?Sucursal", "?Ventas", "?Dia")
				.predicate(new ToJSON(), "?Sucursal", "?Ventas", "?Dia").out("?json");
		Api.execute(new batchview(), tobatchview);
		//Api.execute(new StdoutTap(), topSucursal);
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
	        	obj.put("Sucursal", val.getString("?Sucursal"));
	        	obj.put("Ventas", val.getString("?Ventas"));
	        	obj.put("Dia", val.getString("?Dia"));
	        	array.add(obj.toString());
	        }
	        call.getOutputCollector().add(new Tuple(array));
		}
	}
}
