package test.sparkconnector;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import info.archinnov.achilles.embedded.CassandraEmbeddedServerBuilder;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;

import scala.collection.IndexedSeq;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.spark.connector.ColumnRef;
import com.datastax.spark.connector.cql.TableDef;
import com.datastax.spark.connector.rdd.reader.RowReader;
import com.datastax.spark.connector.rdd.reader.RowReaderFactory;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ){
    	populateDatabase();
    	readWithSpark();
    }
    
    private static void populateDatabase(){
    	Session session = null;
    	try {
    		 CassandraEmbeddedServerBuilder
    	        .builder()
    	        .cleanDataFilesAtStartup(true)
    	        .withCQLPort(9042)
    	        .buildServer();
    		
			Cluster cluster = new Cluster.Builder().addContactPoints(new String[]{"127.0.0.1"}).withPort(9042).build();
			session = cluster.connect();
			session.execute("DROP KEYSPACE IF EXISTS spark_connector_test;");
			session.execute("CREATE KEYSPACE IF NOT EXISTS spark_connector_test WITH replication={'class' : 'SimpleStrategy', 'replication_factor':1};");
			session.execute("use spark_connector_test;");
			session.execute("CREATE TABLE temp_table(id uuid,  x0 double, y0 double, PRIMARY KEY(id));");
			session.execute("INSERT INTO temp_table (id, x0, y0) VALUES(324193a0-1ee5-11e4-b871-2c44fd2578e0, 1.0, 2.0);");
		} catch (Throwable e) {
			e.printStackTrace();
		} finally{
			if(session!=null){
				session.close();
			}
		}
    	
    }

    private static void readWithSpark(){
    	SparkContext context = null;
    	try{
    		SparkConf sparkConf = new SparkConf()
        	.setMaster("local[*]")
        	.setAppName("testApp")
        	.setSparkHome("")
        	.set("spark.cores.max","2")
        	.set("spark.executor.memory", "128m")
        	.set("spark.cassandra.connection.host", "127.0.0.1")
        	.set("spark.cassandra.connection.port", "9042")
        	.set("spark.io.compression.codec","lzf");
        	
        	context = new SparkContext("local[*]", "testJob", sparkConf);

        	RowReaderFactory<TestBean> factory = new RowReaderFactory<TestBean>() {
    			@Override
    			public Class<TestBean> targetClass() {
    				return TestBean.class;
    			}
    			@Override
    			public RowReader<TestBean> rowReader(TableDef def, IndexedSeq<ColumnRef> sequence) {
    				return new TestBeanReader();
    			}
    		};
    		JavaRDD<TestBean> javaBeans = javaFunctions(context).cassandraTable("spark_connector_test", "temp_table", factory).select("id", "x0", "y0");
    		System.out.println(javaBeans.collect());
    	}catch(Throwable e){
    		e.printStackTrace();
    	}finally{
    		if(context!=null){
    			context.stop();
    		}
    	}
    }
}
