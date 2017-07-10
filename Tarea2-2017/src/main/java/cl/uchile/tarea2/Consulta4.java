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
import cl.uchile.tarea2.Consulta2.ToJSON;
import jcascalog.Api;
import jcascalog.Option;
import jcascalog.Subquery;
import jcascalog.op.Count;
import jcascalog.op.DistinctCount;
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

public class Consulta4 {
	//Consulta 3: En que sucursales se hacen más ventas
	
	private static String dia = new SimpleDateFormat("yyyy-MM-dd").format(Calendar.getInstance().getTime());
	private static String sucursal = "EjlMaW5jb3nDoW4gMTYxOS0xNzA3LCBJcXVpcXVlLCBSZWdpw7NuIGRlIFRhcmFwYWPDoSwgQ2hpbGU\"";
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
	
	public static class ExtractBoletas extends CascalogFunction {
		public void operate(FlowProcess process, FunctionCall call) {
			Data data =((Data)call.getArguments().getObject(0));
			String boletaId = data.getDataunit().getBoleta_property().getId().getBoleta_id();
			call.getOutputCollector().add(new Tuple (boletaId, dia));
		}
	}
	
	//esta funcion entrega (esperemos) la sucursal con mas ventas
	public static void topSucursalVentas() {
		PailTap BoletaId = getDataTap("C:/Paula/masterset/data/4");
		
		Subquery sucursalInfo = new Subquery("?BoletaId","?Total", "?Dia")
				.predicate(BoletaId, "_", "?data")
				.predicate(new ExtractBoletas(), "?data").out("?BoletaId", "?Dia")
				.predicate(new Count(),"?Total");
		
		Subquery tobatchview = new Subquery("?json")
				.predicate(sucursalInfo, "?BoletaId","?Total", "?Dia")
				.predicate(new ToJSON(), "?BoletaId","?Total", "?Dia").out("?json");
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
	        	obj.put("Boleta", val.getString("?BoletaId"));
	        	obj.put("Total", val.getString("?Total"));
	        	obj.put("Dia", val.getString("?Dia"));
	        	array.add(obj.toString());
	        }
	        call.getOutputCollector().add(new Tuple(array));
		}
	}
}
