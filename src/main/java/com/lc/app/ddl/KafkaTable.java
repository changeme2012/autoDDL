package com.lc.app.ddl;

import cn.hutool.core.collection.CollUtil;
import com.lc.app.interfac.KafkaInterface;
import com.lc.bean.BackendTable;
import com.lc.bean.tableBean;
import com.lc.common.MyConfiguration;
import com.lc.common.SqlConfigFactory;
import com.lc.utils.DDLUtil;
import com.lc.utils.DruidConnectUtil;
import com.lc.utils.MyDBHelper;
import lombok.SneakyThrows;

import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * ClassName:KafkaTable
 * Package:com.lc.app
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-05-11-10:59
 */
public class KafkaTable implements KafkaInterface {

    private SqlConfigFactory configFactory = null;

//    private static final StringBuffer sql = new StringBuffer();

    private static String resultsSQL;

    private static String joinStr;

    private static List<tableBean> resultList;

    private static List<BackendTable> backendTableResultList;

    private static List<String> columnListl;


    private static MyDBHelper dbHelper;

    static {
        dbHelper = DruidConnectUtil.getMysqlDBHelper();
}

    public static SqlSourceBuilder builder(){
        return new SqlSourceBuilder();
    }

    public KafkaTable(SqlConfigFactory configFactory) {
        this.configFactory = configFactory;
    }


//    @Override
//    public void create() {
//        resultsSQL =String.format(configFactory.getKafkaSourceWith(),configFactory.getMaxwelSourceColumn());
//    }


    @Override
    public void qurey() throws Exception {
        if (configFactory.getSelect() == null){
            String database = configFactory.getDatabase();
            String table = configFactory.getTable();

            try {
                resultList = DDLUtil.getResultSet(database, table);
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        }{
            String tableName = configFactory.getSelect();

            backendTableResultList = dbHelper.queryList(MyConfiguration.BACKEND_TABLE_SQL, BackendTable.class, true,tableName);

        }


    }

    @Override
    public void allcolumn() {
        columnListl = new ArrayList<>();
        for (tableBean tableBean : resultList) {
            String columnName = tableBean.getColumnName();
            columnListl.add(columnName);
        }
    }

    @Override
    public void exclude() {
            String[] excludes = configFactory.getExclude().split(",");
            HashSet<String> hashSet = new HashSet<>(Arrays.asList(excludes));
            resultList.removeIf(tableBean -> hashSet.contains(tableBean.getColumnName()));

    }



    @Override
    public void spliceSQL() {
        ArrayList<String> templList = new ArrayList<>();

        if (configFactory.getFilterTable() != null ){
            columnListl = resultList.stream().map(tableBean -> {

                return "\tdata['" + tableBean.getColumnName() + "'] " + tableBean.getColumnName();

            }).collect(Collectors.toList());
        }else if(configFactory.getSelect() != null){
            BackendTable backendTable = backendTableResultList.get(0);
            String[] split = backendTable.getColumns().split(",");

            for (String column : split) {
                templList.add("\t"+column);
            }



            columnListl=templList;
        }
    }


    @Override
    public void include() {

        if (configFactory.getInclude() != null){
                String filterTable = configFactory.getInclude();
                String[] split = filterTable.split(",");
                List<String> list = Arrays.asList(split);
                resultList.removeIf(tableBean -> !list.contains(tableBean.getColumnName()));
        }
    }

    @Override
    public List<String> function() {

        //获取function的值
        HashMap<String, String[]> functionMap = configFactory.getFunction();

        //用于存放修改过的list
            List<String> modifiedColumnList = new ArrayList<>();

            Set<Map.Entry<String, String[]>> entries = functionMap.entrySet();
            //遍历functionMap获取k,v
            for (Map.Entry<String, String[]> entry : entries) {
                //获取要用函数的字段名
                String key = entry.getKey();
                //获取函数模板
                String[] functions = entry.getValue();
                //当传入的函数模板只有1个时，UNIX_TIMESTAMP(%s)时
                if (functions.length < 2) {



                    //遍历上游处理过的columnListl
                    for (String column : columnListl) {

                        //如果column包含传入的要用函数的字段名，再去判断column是FilterTable的多列字段，还是select的单列字段
                        if (column.contains(key)) {
                            //String.format( UNIX_TIMESTAMP(%s),column) ==UNIX_TIMESTAMP(column) ;
                            if (configFactory.getFilterTable() != null) {

                            String[] split = column.split("\\s");
//                        System.out.println("split>>>1:"+split[split.length-2]);
                                split[split.length - 2] = String.format(functions[0], split[split.length - 2]);
//                        System.out.println("split>>>2:"+split[split.length-2]);
                                modifiedColumnList.add("\t" + split[split.length - 2] + " " + split[split.length - 1]);
                            }
                            else if(configFactory.getSelect() != null){
//                                System.out.println(column);

                                if (column.contains(",")){

                                    String newColumn = String.format(functions[0], column.charAt(column.lastIndexOf(",") - 1));

                                    modifiedColumnList.add("\t" + newColumn );

                                }else {

                                    String newColumn = String.format(functions[0], column );

                                    modifiedColumnList.add("\t" + newColumn );

                                }


                            }
                        } else {
                            modifiedColumnList.add(column);
                        }
                    }
                }

                //超过一个函数
                else {
                    for (String column : columnListl) {
                        if (column.contains(key)) {
                            for (int i = 0; i < functions.length; i++) {

                                String[] split = column.split("\\s");

                                split[0] = String.format(functions[i], split[0]);

                            }
                        }
                    }
                }

                columnListl = modifiedColumnList;
                return columnListl;
            }{
                return null;
        }

    }

    @Override
    public void encapsulatedSQL() {

        if (configFactory.getFilterTable() != null){
            resultsSQL =String.format(configFactory.getFilterTable(),CollUtil.join(columnListl,",\n"));
        }else if (configFactory.getSelect() != null){
            String tableName = configFactory.getSelect();
            resultsSQL = String.format(MyConfiguration.QUERY, CollUtil.join(columnListl, ",\n"),tableName);
        }else if (configFactory.getKafkaSinkWith() != null){
            resultsSQL = String.format(configFactory.getKafkaSinkWith(), CollUtil.join(columnListl, ",\n"));
        }else if (configFactory.getUpsertKafkaSinkWith() != null){
            resultsSQL = String.format(configFactory.getUpsertKafkaSinkWith(), CollUtil.join(columnListl, ",\n"));
        }
        else {
            resultsSQL = String.format(configFactory.getKafkaSourceWith(), CollUtil.join(columnListl, ",\n"));
        }
    }

    @Override
    public String where() {

        return null;
    }

    @Override
    public void join() {
        if (configFactory.getJoin() != null) {
            //处理JOIN 后需要的字段


            //处理JOIN 相关字段
            HashMap<String, String> join = configFactory.getJoin();

            Set<Map.Entry<String, String>> entries = join.entrySet();

            for (Map.Entry<String, String> entry : entries) {
               //处理表 别名
                String key = entry.getKey();
                //取表别名,取分隔后每段字符的第一个字符
                String[] parts = key.split("_");
                StringBuilder tableName = new StringBuilder();
                for (String part : parts) {
                    if (!part.isEmpty()) {
                        tableName.append(part.charAt(0));
                    }
                }
            //处理 on 字段
                String value = entry.getValue();

                joinStr = String.format(MyConfiguration.JOIN, key, tableName, value);

            }

        }

    }

    @Override
    public String lookpuJoin() {
        return null;
    }

    @SneakyThrows
    @Override
    public void save() {

        if(configFactory.getSaveTableAlias() != null) {

            String tableAlias = configFactory.getSaveTableAlias();

            ArrayList<String> columns = new ArrayList<>();

            for (String column : columnListl)
            {
                String[] split = column.split("\\s");

                columns.add(split[split.length-1].trim());
            }

//            System.out.println(columns);

            long currentTimeMillis = System.currentTimeMillis();

            Timestamp ts = new Timestamp(currentTimeMillis);

//            String sql =String.format( "INSERT INTO %s VALUES(?,?,?)",MyConfiguration.BACKEND_TABLE);

            BackendTable backendTable = new BackendTable(tableAlias, String.join(",",columns), ts);

            dbHelper.insert(MyConfiguration.BACKEND_TABLE,backendTable);
        }
    }

    @Override
    public void print() throws Exception {
  /*      if (configFactory.getKafkaSourceWith() != null ){
                  create();
        }*/
        if (configFactory.getSelect() != null || configFactory.getFilterTable() != null){
            qurey();
        }
       if (configFactory.getExclude() != null){
           exclude();
       }
       if (configFactory.getInclude() != null){
           include();
       }


//          allcolumn();


        spliceSQL();

       if (configFactory.getFunction() != null || configFactory.getFunction().isEmpty() ){
           function();
       }

        encapsulatedSQL();

       if (configFactory.getSaveTableAlias() != null){
           save();
       }

       if (configFactory.getSelect() != null){

       }

        System.out.println(resultsSQL);
    }
}
