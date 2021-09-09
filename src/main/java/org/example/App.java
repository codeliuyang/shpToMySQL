package org.example;

import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.geotools.data.DataStore;
import org.geotools.data.DataStoreFinder;
import org.geotools.data.FeatureWriter;
import org.geotools.data.Transaction;
import org.geotools.data.mysql.MySQLDataStoreFactory;
import org.geotools.data.shapefile.ShapefileDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureIterator;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.jdbc.JDBCDataStore;

import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

/**
 *
 *
 * Notes
 * (1) 除了设置读取shp文件时的编码为GBK，同时需要设置MySQL的一个配置 character-set-server=utf8.
 *     否则，依然会发现存在乱码问题。因为JDBC连接MySQL一般jdbc:url...都要指定编码utf8
 * (2)
 */
public class App{


    public static SimpleFeatureSource readSHP( String shpfile){
        SimpleFeatureSource featureSource =null;
        try {
            File file = new File(shpfile);
            ShapefileDataStore shpDataStore = null;

            shpDataStore = new ShapefileDataStore(file.toURL());
            //设置编码
            Charset charset = Charset.forName("GBK");
            shpDataStore.setCharset(charset);
            String tableName = shpDataStore.getTypeNames()[0];
            featureSource =  shpDataStore.getFeatureSource (tableName);
        }catch (Exception e){
            e.printStackTrace();
        }
        return featureSource;
    }

    public static JDBCDataStore connnection2mysql(String host, String dataBase, int port, String userName, String pwd) {
        JDBCDataStore ds = null;
        DataStore dataStore = null;
        // 连接数据库参数
        Map<String,Object> params = new HashMap<>();
        params.put(MySQLDataStoreFactory.DBTYPE.key, "mysql");
        params.put(MySQLDataStoreFactory.HOST.key, host);
        params.put(MySQLDataStoreFactory.PORT.key, port);
        params.put(MySQLDataStoreFactory.DATABASE.key, dataBase);
        params.put(MySQLDataStoreFactory.USER.key, userName);
        params.put(MySQLDataStoreFactory.PASSWD.key, pwd);
        params.put(MySQLDataStoreFactory.STORAGE_ENGINE.key, "InnoDB");
        try {
            dataStore = DataStoreFinder.getDataStore(params);
            if (dataStore != null) {
                ds = (JDBCDataStore) dataStore;
                System.out.println(dataBase + "连接成功");
            } else {

                System.out.println(dataBase + "连接失败");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }

        return ds;
    }


    public static JDBCDataStore createTable(JDBCDataStore ds, SimpleFeatureSource featureSource){
        SimpleFeatureType schema = featureSource.getSchema();
        try {
            //创建数据表
            ds.createSchema(schema);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        return ds;
    }


    public static void writeShp2Mysql(JDBCDataStore ds, SimpleFeatureSource featureSource ){
        SimpleFeatureType schema = featureSource.getSchema();
        //开始写入数据
        try {
            FeatureWriter<SimpleFeatureType, SimpleFeature> writer = ds.getFeatureWriter(
                    schema.getTypeName().toLowerCase(),
                    Transaction.AUTO_COMMIT);
            SimpleFeatureCollection featureCollection = featureSource.getFeatures();
            SimpleFeatureIterator features = featureCollection.features();
            System.out.println("开始导入...");
            while (features.hasNext()) {
                writer.hasNext();
                SimpleFeature next = writer.next();
                SimpleFeature feature = features.next();
                for (int i = 0; i < feature.getAttributeCount(); i++) {
                    next.setAttribute(i,feature.getAttribute(i) );
                }
                writer.write();
            }
            writer.close();
            ds.dispose();
            System.out.println("导入成功!");
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public static List<String> getSHPFiles(String path){
        File shpDir = new File(path);
        String[] fileNameLists = shpDir.list(); //存储文件名的String数组
        List<String> rtns = new ArrayList<>();
        for(String fileName: fileNameLists){
            if (fileName.endsWith(".shp")){
                rtns.add(path + "/" + fileName);
                System.out.println(path + "/" + fileName);
            }
        }
        return rtns;
    }


    //测试代码
    public static void main(String[] args) {
        List<String> files = getSHPFiles("/Users/yangliu/Documents/05demos/shptomysql/shps");
        for(String path: files){
            System.out.println(">>>> 准备处理文件：" + path);
            JDBCDataStore newDataStore = connnection2mysql(
                    "192.168.1.11",
                    "gis_c1",
                    3309,
                    "root",
                    "mysql123!");
            SimpleFeatureSource featureSource = readSHP(path);
            JDBCDataStore ds = createTable(newDataStore, featureSource);
            writeShp2Mysql(ds, featureSource);
            System.out.println(">>>> 完成处理文件：" + path);
        }

    }



}
