package com.lc.app.ddl;

import com.lc.common.SqlConfigFactory;
import lombok.Data;

/**
 * ClassName:SqlSourceBuilder
 * Package:com.lc.app
 * Description:
 *
 * @Author: 龙成
 * @Create: 2023-05-11-11:11
 */

@Data
public class SqlSourceBuilder {

//   private  String kafkaSourceWith;
//   private  String kafkaSinkWith;
//   private  String upsertKafkaSinkWith;


    private final SqlConfigFactory configFactory = new SqlConfigFactory();

    public SqlSourceBuilder exclude(String exclude) {
        this.configFactory.exclude(exclude);
        return this;
    }

    public SqlSourceBuilder include(String include) {
        this.configFactory.include(include);
        return this;
    }

    public SqlSourceBuilder columnExp(String columnExp,String...function) {
        this.configFactory.columnExp(columnExp,function);
        return this;
    }

    public SqlSourceBuilder function(String column,String...function) {
        this.configFactory.function(column,function);
        return this;
    }

    public SqlSourceBuilder where(String where) {
        this.configFactory.where(where);
        return this;
    }

    public SqlSourceBuilder join(String joinTableName,String onField) {
        this.configFactory.join(joinTableName,onField);
        return this;
    }

    public SqlSourceBuilder lookupJoin(String lookupJoin) {
        this.configFactory.lookupJoin(lookupJoin);
        return this;
    }


    public SqlSourceBuilder database(String database) {
        this.configFactory.database(database);
        return this;
    }

    public SqlSourceBuilder table(String table) {
        this.configFactory.table(table);
        return this;
    }

    public SqlSourceBuilder saveTableAlias(String saveTableAlias) {
        this.configFactory.saveTableAlias(saveTableAlias);
        return this;
    }

    public SqlSourceBuilder topic(String topic) {
        this.configFactory.topic(topic);
        return this;
    }

    public SqlSourceBuilder groupId(String groupId) {
        this.configFactory.groupId(groupId);
        return this;
    }

    public SqlSourceBuilder scanMode(String scanMode) {
        this.configFactory.scanMode(scanMode);
        return this;
    }

    public SqlSourceBuilder filterTable(String fromTableName,String filterTableName,String type) {
        this.configFactory.filterTable(fromTableName,filterTableName,type);
        return this;
    }

    public SqlSourceBuilder select(String fromTableName) {
        this.configFactory.select(fromTableName);
        return this;
    }

    public SqlSourceBuilder maxwelSourceColumn() {
        this.configFactory.maxwelSourceColumn();
        return this;
    }

    public SqlSourceBuilder kafkaSourceWith(String createTableName) {
        this.configFactory.kafkaSourceWith(createTableName);
        return this;
    }

    public SqlSourceBuilder kafkaSinkWith(String createTableName) {
        this.configFactory.kafkaSinkWith(createTableName);
        return this;
    }

    public SqlSourceBuilder upsertKafkaSinkWith(String createTableName) {
        this.configFactory.upsertKafkaSinkWith(createTableName);
        return this;
    }

    public KafkaTable build() {
        return new KafkaTable(configFactory);
    }

}
