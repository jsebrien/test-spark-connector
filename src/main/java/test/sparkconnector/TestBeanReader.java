package test.sparkconnector;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import scala.Option;
import scala.collection.JavaConversions;
import scala.collection.Seq;

import com.datastax.driver.core.Row;
import com.datastax.spark.connector.ColumnName;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.rdd.reader.RowReader;

public class TestBeanReader implements RowReader<TestBean>{

	private static final long serialVersionUID = 1L;
	
	private final Seq<ColumnRef> columnNames;
	
	public TestBeanReader(){
		List<ColumnRef> columns = new ArrayList<>();
		columns.add(new ColumnName("id", null));
		columns.add(new ColumnName("x0", null));
		columns.add(new ColumnName("y0", null));
		this.columnNames = JavaConversions.asScalaBuffer(columns);
	}
	
	public Option<Seq<ColumnRef>> neededColumns() {
		return Option.apply(columnNames);
	}

	public TestBean read(Row row, String[] columnNames) {
		UUID id = row.getUUID("id");
		double x0 = row.getDouble("x0");
		double y0 = row.getDouble("y0");
		return new TestBean(id, x0, y0);
	}

}
