package nudt.pdl.stormwindow.util;

import java.util.ArrayList;

import backtype.storm.tuple.Values;
import nudt.pdl.stormwindow.event.Attribute;
import nudt.pdl.stormwindow.event.IEventType;

import nudt.pdl.stormwindow.event.TupleEventType;

public class Constant {
	public static final String DEFAULT_INPUT_STREAM = "default";
	
	public static final IEventType DEFAULT_INPUT_SCHMA = new TupleEventType(DEFAULT_INPUT_STREAM, new Attribute(Object.class, "datas" ));

	public static final String DEFAULT_OUTPUT_STREAM = "default";
	
	public static final IEventType DEFAULT_OUTPUT_SCHMA = new TupleEventType(DEFAULT_OUTPUT_STREAM, new Attribute(Object.class, "datas" ));
}
