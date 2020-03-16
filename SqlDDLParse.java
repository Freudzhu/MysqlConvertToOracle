import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.ConversionException;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class SqlDDLParse{


    private static TreeMap<String, String> replacements = new TreeMap<>();

    static {
        // oracle 不支持`
        replacements.put("`.*?`\\.", "");
        //类型替换begin
        replacements.put("VARCHAR", "VARCHAR2");
        replacements.put("NOT NULL", "NOT NULL ENABLE");
        replacements.put("INT", " NUMBER");
        replacements.put("BIGINT", "NUMBER");
        replacements.put("INTEGER", "NUMBER");
        replacements.put("BINARY", "RAW");
        replacements.put("BIT", " RAW");
        replacements.put("BOOLEAN", " CHAR");
        replacements.put("BOOL", " CHAR");
        replacements.put("DECIMAL", " NUMBER");
        replacements.put("DOUBLE", " BINARY_DOUBLE");
        replacements.put("FIXED", " NUMBER");
        replacements.put("FLOAT", "BINARY_DOUBLE");
        replacements.put("FLOAT4", "BINARY_DOUBLE");
        replacements.put("LONGBLOB", "BLOB");
        replacements.put("LONGTEXT", "CLOB");
        replacements.put("LONG VARCHAR", "CLOB");
        replacements.put("MEDIUMBLOB", "BLOB");
        replacements.put("MEDIUMINT", "NUMBER");
        replacements.put("MIDDLEINT", "NUMBER");
        replacements.put("NVARCHAR", "NVARCHAR2");
        replacements.put("SMALLINT", "NUMBER");
        replacements.put("TEXT", " CLOB");
        replacements.put("TINYBLOB", "RAW");
        replacements.put("TINYINT", "NUMBER");
        replacements.put("TINYTEXT", "VARCHAR2");
        replacements.put("VARBINARY", "RAW");
        replacements.put("YEAR", " NUMBER");
        replacements.put("NUMERIC", "NUMBER");
        //类型替换over
        //语法替换begin
        replacements.put("ENGINE( )?=( )?.*", "");
        replacements.put("ADD COLUMN", "");
        replacements.put("AFTER \".*?\"", "");
        replacements.put("BEFORE \".*?\"", "");
        replacements.put("IF NOT EXISTS .\\w*.\\.", "");
        replacements.put("DATETIME", "TIMESTAMP");
        replacements.put("ON DELETE NO ACTION", "");
        replacements.put("ON UPDATE NO ACTION", "");
        replacements.put(".mydb..", "");
        replacements.put("\"\\w+\"\\..*?", "");
        replacements.put("CREATE SCHEMA IF NOT EXISTS DEFAULT CHARACTER", "");
        replacements.put("\\s*?\\)", ")");
        replacements.put("AUTO_INCREMENT", "");
//       replacements.put("USE.*?,",",");
        replacements.put("SET.*?([\\,\\)])", "$1");
        replacements.put("ON UPDATE.*?,", ",");
        replacements.put("\\;", "/");
        replacements.put("CHARACTER", "");
        //remove primary key
        replacements.put(",\\s+PRIMARY KEY.*(\\))", " $1");
        replacements.put("PRIMARY KEY (AUTO_INCREMENT)*","");
        //remove comment
        replacements.put("COMMENT .*?'\\S.*?'", "");
        replacements.put("DEFAULT (?!NULL)\\S.*?([,)])", "");
        replacements.put("UNSIGNED", "");

    }

    public String getOracleSql(TGssDbDdl tGssDbDdl) {
        String formatSql = formatSql(tGssDbDdl.getDdlSql());
        return getSqlScripts(tGssDbDdl, formatSql);
    }



    private String getSqlScripts(TGssDbDdl tGssDbDdl, String formatSql) {
        //判断操作类型
        String eventType = tGssDbDdl.getEventType();
        //truncate
        if ("TRUNCATE".equalsIgnoreCase(eventType)) {
            return formatSql.replaceAll("(?i)IF EXISTS .\\w*.*", "").replaceAll("\".*?\"\\.", "");
        }
        //drop
        if ("ERASE".equalsIgnoreCase(eventType)) {
//            upperCaseSql = upperCaseSql.replaceAll("IF EXISTS .*", "");
//            return upperCaseSql.replaceAll("IF EXISTS .\\w*.*", "").replaceAll("\".*?\"\\.", "");
            return "DROP TABLE " + tGssDbDdl.getTableName();
        }
        //ALTER
        if ("ALTER".equalsIgnoreCase(eventType)) {
            String tmpSql = getSQLByAlter(tGssDbDdl, formatSql);
            if (tmpSql != null) return tmpSql;
        }
        //create
        if ("CREATE".equalsIgnoreCase(eventType)) {
            //主键SQL
            String alterSqL = getPrimaryKeySql(tGssDbDdl);
            //注释SQL+建表语句SQL
            String createSql = getCreateSql(tGssDbDdl);

            return createSql  + alterSqL;
        } else {
            throw new ConversionException("DDL同步未知的事件类型:" + eventType);
        }
    }

    private String getPrimaryKeySql(TGssDbDdl tGssDbDdl) {
        return "ALTER TABLE " + tGssDbDdl.getTableName() + " ADD CONSTRAINT PK_" + tGssDbDdl.getTableName().toUpperCase() + " PRIMARY KEY (\"ID\")";
    }

    private String getSQLByAlter(TGssDbDdl tGssDbDdl, String upperCaseSql) {
        //ADD
        Pattern p = Pattern.compile("ALTER TABLE.*?ADD",Pattern.CASE_INSENSITIVE);
        if (p.matcher(upperCaseSql).find()) {
            String tmpSql = upperCaseSql.replaceAll("(?i)ALTER TABLE.*?ADD (COLUMN)?", "");
            tmpSql = "ALTER TABLE " + tGssDbDdl.getTableName() + " ADD  (" + tmpSql.replaceAll("(?i)ADD (COLUMN)?", "") + ")";
            tmpSql = replaceStr(tmpSql);
            return tmpSql;
        }
        //MODIFY
        p = Pattern.compile("ALTER TABLE.*?MODIFY",Pattern.CASE_INSENSITIVE);
        if (p.matcher(upperCaseSql).find()) {
            String tmpSql = upperCaseSql.replaceAll("(?i)ALTER TABLE.*?MODIFY (COLUMN)?", "");
            tmpSql = "ALTER TABLE " + tGssDbDdl.getTableName() + " MODIFY  (" + tmpSql.replaceAll("(?i)MODIFY (COLUMN)?", "") + ")";
            for (Map.Entry<String, String> vkPairs : replacements.entrySet()) {
                tmpSql = tmpSql.replaceAll("(?s)" + vkPairs.getKey(), vkPairs.getValue());
                tmpSql = tmpSql.replaceAll("(?i) NULL ", " ").replaceAll("(?i) DEFAULT "," ").replaceAll("(?i) NOT "," ");
            }
            return tmpSql;
        }
        //DROP
        p = Pattern.compile("ALTER TABLE.*?DROP",Pattern.CASE_INSENSITIVE);
        if (p.matcher(upperCaseSql).find()) {
            String tmpSql = upperCaseSql.replaceAll("(?i)ALTER TABLE.*?DROP (COLUMN)?", "");
            tmpSql = "ALTER TABLE " + tGssDbDdl.getTableName() + " DROP  (" + tmpSql.replaceAll("(?i)DROP (COLUMN)?", "") + ")";
            return tmpSql;
        }
        //CHANGE
        p = Pattern.compile("ALTER TABLE.*?CHANGE",Pattern.CASE_INSENSITIVE);
        if (p.matcher(upperCaseSql).find()) {
            String tmpSql = upperCaseSql.replaceAll("(?i)ALTER TABLE.*?", "");
//            for (Map.Entry<String, String> vkPairs : replacements.entrySet()) {
//                tmpSql = tmpSql.replaceAll("(?s)" + vkPairs.getKey(), vkPairs.getValue());
//            }
            p = Pattern.compile("CHANGE (COLUMN )*((([\"']{0,1}\\w+['\"]{0,1})\\s)*)",Pattern.CASE_INSENSITIVE);
            Matcher matcher = p.matcher(tmpSql);
            List<String> args = new ArrayList<>();
            while (matcher.find()) {
                args.add(matcher.group(2));
            }
            String resultSql = "";
            for (int i = 0; i < args.size();i++) {
                String oldColumn = args.get(i).split(" ")[0];
                String newColumn = args.get(i).split(" ")[1];
                resultSql += "ALTER TABLE " + tGssDbDdl.getTableName() + " RENAME COLUMN " + oldColumn + " to " + newColumn + ";";
            }
            return resultSql;
        }
        throw new ConversionException("DDL同步未知的ALTER语句:" + tGssDbDdl.getDdlSql());
    }

    private String replaceStr(String tmpSql) {
        for (Map.Entry<String, String> vkPairs : replacements.entrySet()) {
            tmpSql = tmpSql.replaceAll("(?s)(?i)" + vkPairs.getKey(), vkPairs.getValue());
        }
        return tmpSql;
    }

   

    private String getCreateSql(TGssDbDdl ddl) {
        List<String> createSql = new ArrayList<>();
        String commentSql = "";
        //去除换行，回车符，`,转大写
        String upperCaseSql = formatSql(ddl.getDdlSql());
        //只取括号中的内容 CREATE TABLE (***) ENGINE = ...
        //去除主键和索引
        String replaces = upperCaseSql.replaceAll(".*?(\\()(.*)(\\)).*", "$2")
                .replaceAll("(?i)\\s*PRIMARY KEY.*(\\)).*", "");
        //去除非语法中的逗号
        Pattern pattern = Pattern.compile("(?i)(.*?COMMENT\\s*)(\\'.*?')",Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(replaces);
        String formatSql = "";
        while (matcher.find()){
            String group = matcher.group(2);
            group = group.replaceAll(",", " ");
            formatSql = formatSql + matcher.group(1) + group;
        }
        //以,分割成每个字段
        if(StringUtils.isBlank(formatSql)){
            return "";
        }
        String[] split = formatSql.split(",");
        for (int i = 0; i < split.length; i++) {
            String s = split[i].trim();
            String substring = s.substring(s.length() - 1);
            //取最后一个字符
            //比如类型为decimal(16,2)，会造成非正常切割，对此类情况进行重组
            if (isNumber(substring) && i + 1 < split.length) {
                //判断下一个是否为断层
                String next = split[i + 1];
                //取第一个字符
                String nextSub = next.substring(0, 1);
                if (isNumber(nextSub)) {
                    s = s + "," + next;
                    i++;
                }
            }

            //字段名称
            String column = s.replaceAll("(?i)\\s*(\\S+)\\s.*", "$1").trim();

            //注释内容
            String comment = s.replaceAll("(?i).*(?<=(?:COMMENT))(?:\\s?)(\\S+)", "$1").trim();

            String typeSql = "";
            if(StringUtils.isBlank(comment)){
                typeSql = s.substring(column.length(), s.toUpperCase().indexOf("COMMENT"));
            }else{
                 typeSql = s.substring(column.length());
            }
            //拼接create 语句
            createSql.add(column + " " + replaceStr(typeSql));
            if(!s.toUpperCase().contains(" COMMENT ")||StringUtils.isBlank(column)||StringUtils.isBlank(comment)){
                continue;
            }
            commentSql += "COMMENT ON COLUMN " + ddl.getTableName() + "." + column + " is " + comment + ";";
        }
        return "CREATE TABLE "+ddl.getTableName()+" ( "+StringUtils.join(createSql,",")+" );"+commentSql;
    }
    private  boolean isNumber(String str){
        try{
            Integer integer = new Integer(str);
            return true;
        }catch (Exception e){
            return false;
        }
    }

    /**
     * remove ` ; \n \r to upper
     * @param sql
     * @return
     */
    private String formatSql(String sql) {
        return sql
                .replaceAll("`", "")
                .replaceAll(";", "")
                .replaceAll("\n", "")
                .replaceAll("\r", "");
    }

    public static void main(String[] args) {
        String sql = "CREATE TABLE `t_test_ddl_a` (\n" +
			"  `ID` varchar(32) NOT NULL comment '物理主键',\n" +
			"  `INTF_SEQ` varchar(40) NOT NULL comment '业务关联编号',\n" +
			"  `CUST_NO` varchar(32) NOT NULL comment '客户编号',\n" +
			"  `GENDER` varchar(2) DEFAULT NULL,\n" +
			"  `GENDER_DESC` varchar(30) DEFAULT NULL comment '性别',\n" +
			"  `BIRTH_DT` varchar(10) DEFAULT NULL comment '出生日期',\n" +
			"  `EDU_DIP` varchar(2) DEFAULT NULL comment '学历',\n" +
			"  `EDU_DIP_DESC` varchar(30) DEFAULT NULL comment '学历',\n" +
			"  `DEGREE` varchar(1) DEFAULT NULL comment '学位',\n" +
			"  `DEGREE_DESC` varchar(30) DEFAULT NULL comment '学位',\n" +
			"  `TAKE_OCC_SITU` varchar(2) DEFAULT NULL comment '就业状况',\n" +
			"  `TAKE_OCC_SITU_DESC` varchar(30) DEFAULT NULL comment '就业状况',\n" +
			"  `EMAIL` varchar(60) DEFAULT NULL comment '电子邮箱',\n" +
			"  `COMM_ADDR` varchar(200) DEFAULT NULL comment '通讯地址',\n" +
			"  `NATION` varchar(3) DEFAULT NULL comment '国籍',\n" +
			"  `NATION_DESC` varchar(30) DEFAULT NULL comment '国籍',\n" +
			"  `DOM_RGST_ADDR` varchar(200) DEFAULT NULL comment '户籍地址',\n" +
			"  `MOBILE_NO_CNT` varchar(1) DEFAULT NULL comment '手机号码个数',\n" +
			"  `CREATE_TIME` datetime NOT NULL comment '创建时间',\n" +
			"  `CREATE_USER` varchar(32) DEFAULT NULL comment '创建人',\n" +
			"  `UPDATE_TIME` datetime NOT NULL comment '修改时间',\n" +
			"  `UPDATE_USER` varchar(32) DEFAULT NULL comment '修改人',\n" +
			"  `DEL_FLAG` varchar(1) NOT NULL comment '逻辑删标志',\n" +
			"  PRIMARY KEY (`CUST_NO`,`ID`) USING BTREE,\n" +
			"  KEY `INDX_INTF_SEQ` (`INTF_SEQ`) USING BTREE comment '业务主键'\n" +
			"\n" +
			") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC comment='身份信息' shardkey=cust_no;";
        SqlDDLParse parse = new SqlDDLParse();
        TGssDbDdl ddl = new TGssDbDdl();
        ddl.setTableName("t_test_ddl_a");
        ddl.setEventType("CREATE");
        ddl.setDdlSql(sql);
        System.out.println(parse.getOracleSql(ddl));
    }
}
