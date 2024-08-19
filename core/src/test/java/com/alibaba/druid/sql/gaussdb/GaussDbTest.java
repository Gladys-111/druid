package com.alibaba.druid.sql.gaussdb;

import com.alibaba.druid.sql.ast.statement.SQLCreateTableStatement;
import com.alibaba.druid.sql.dialect.gaussdb.parser.GaussDbCreateTableParser;
import com.alibaba.druid.sql.dialect.gaussdb.parser.GaussDbStatementParser;
import junit.framework.TestCase;
import org.junit.Test;

public class GaussDbTest extends TestCase {

    @Test
    public void testPartition1() {
        String sql = "CREATE TABLE test_range1(\n" +
                "    id INT, \n" +
                "    info VARCHAR(20)\n" +
                ") PARTITION BY RANGE (id) (\n" +
                "    PARTITION p1 VALUES LESS THAN (200) TABLESPACE tbs_test_range1_p1,\n" +
                "    PARTITION p2 VALUES LESS THAN (400) TABLESPACE tbs_test_range1_p2,\n" +
                "    PARTITION p3 VALUES LESS THAN (600) TABLESPACE tbs_test_range1_p3,\n" +
                "    PARTITION pmax VALUES LESS THAN (MAXVALUE) TABLESPACE tbs_test_range1_p4\n" +
                ");";
        GaussDbStatementParser parser = new GaussDbStatementParser(sql);
        SQLCreateTableStatement parsed = parser. parseCreateTable();
        final String result = parsed.toString();
        System.out.println(result);
    }

    @Test
    public void testPartition2() {
        String sql = "CREATE TABLE test_range2(\n" +
                "    id INT, \n" +
                "    info VARCHAR(20)\n" +
                ") PARTITION BY RANGE (id) (\n" +
                "    PARTITION p1 START(1) END(600) EVERY(200),    \n" +
                "    PARTITION p2 START(600) END(800),\n" +
                "    PARTITION pmax START(800) END(MAXVALUE)\n" +
                ");";
        GaussDbStatementParser parser = new GaussDbStatementParser(sql);
        SQLCreateTableStatement parsed = parser.parseCreateTable();
        final String result = parsed.toString();
        System.out.println(result);
    }

    @Test
    public void testPartition3() {
        String sql = "CREATE TABLE test_list ( NAME VARCHAR ( 50 ), area VARCHAR ( 50 ) ) \n" +
                "PARTITION BY LIST (area) (\n" +
                "    PARTITION p1 VALUES ('Beijing'),\n" +
                "    PARTITION p2 VALUES ('Shanghai'),\n" +
                "    PARTITION p3 VALUES ('Guangzhou'),\n" +
                "    PARTITION p4 VALUES ('Shenzhen'),\n" +
                "    PARTITION pdefault VALUES (DEFAULT)\n" +
                ");";
        GaussDbStatementParser parser = new GaussDbStatementParser(sql);
        SQLCreateTableStatement parsed = parser.parseCreateTable();
        final String result = parsed.toString();
        System.out.println(result);
    }

    @Test
    public void testDistributed(){
        String sql = "CREATE TABLE test_range(\n" +
                "    id       INT,\n" +
                "    name     VARCHAR(20),\n" +
                "    province VARCHAR(60),                       \n" +
                "    country   VARCHAR(30) DEFAULT 'China'       \n" +
                ")DISTRIBUTE BY RANGE(id)(\n" +
                "    SLICE s1 START (100) END (200) EVERY (10),\n" +
                "    SLICE s2 START (200) END (300) EVERY (10)\n" +
                ");";
        GaussDbCreateTableParser gaussDbCreateTableParser = new GaussDbCreateTableParser(sql);
        final SQLCreateTableStatement parsed = gaussDbCreateTableParser.parseCreateTable();
        final String result = parsed.toString();
        System.out.println(result);
    }

    @Test
    public void testDistributedRange() {
        String sql = "CREATE TABLE test_range(\n" +
                "    id       INT,\n" +
                "    name     VARCHAR(20),\n" +
                "    province VARCHAR(60),                       --省\n" +
                "    country   VARCHAR(30) DEFAULT 'China'        --国籍\n" +
                ")DISTRIBUTE BY RANGE(id)(\n" +
                "    SLICE s1 VALUES LESS THAN (100) DATANODE dn_6001_6002_6003,\n" +
                "    SLICE s2 VALUES LESS THAN (200) DATANODE dn_6004_6005_6006,\n" +
                "    SLICE s3 VALUES LESS THAN (MAXVALUE) DATANODE dn_6007_6008_6009\n" +
                ");";
        GaussDbCreateTableParser gaussDbCreateTableParser = new GaussDbCreateTableParser(sql);
        final SQLCreateTableStatement parsed = gaussDbCreateTableParser.parseCreateTable();
        final String result = parsed.toString();
        System.out.println(result);
    }

    @Test
    public void testDistributedList(){
        String sql = "CREATE TABLE local_autoinc(col int, col1 int)\n" +
                "DISTRIBUTE BY LIST(col1)(\n" +
                "    SLICE s1 VALUES (1) DATANODE datanode1,\n" +
                "    SLICE s2 VALUES (2) DATANODE datanode2\n" +
                ");";
        GaussDbCreateTableParser gaussDbCreateTableParser = new GaussDbCreateTableParser(sql);
        final SQLCreateTableStatement parsed = gaussDbCreateTableParser.parseCreateTable();
        final String result = parsed.toString();
        System.out.println(result);
    }

    @Test
    public void testTableOption(){
        String sql = "CREATE TABLE `test1` (\n" +
                "  `id` bigint(20) NOT NULL AUTO_INCREMENT COMMENT 'id',\n" +
                "  `c_tinyint` tinyint(4) DEFAULT '1' COMMENT 'tinyint',\n" +
                "  `c_timestamp` timestamp NULL DEFAULT NULL COMMENT 'timestamp' ON UPDATE CURRENT_TIMESTAMP(),\n" +
                "  `c_time` time DEFAULT NULL COMMENT 'time',\n" +
                "  `c_char` char(10) DEFAULT NULL COMMENT 'char',\n" +
                "  `c_varchar` varchar(10) DEFAULT 'hello' COMMENT 'varchar',\n" +
                "  `c_longblob` longblob COMMENT 'longblob',\n" +
                "  PRIMARY KEY (`id`,`c_tinyint`)\n" +
                ") " +
                "COMMENT='10000000'" +
                "AUTO_INCREMENT=1769503 " +
                "DEFAULT CHARACTER SET 'utf8mb4' " +
                "ENGINE=InnoDB ";
        GaussDbCreateTableParser gaussDbCreateTableParser = new GaussDbCreateTableParser(sql);
        final SQLCreateTableStatement parsed = gaussDbCreateTableParser.parseCreateTable();
        final String result = parsed.toString();
        System.out.println(result);
    }

}
