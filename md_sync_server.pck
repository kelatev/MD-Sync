CREATE OR REPLACE PACKAGE md_sync_server IS

  -- Author  : KELATEV
  -- Created : 14.07.2017 10:16:53
  -- Purpose : 

  PROCEDURE xml(p_code      IN VARCHAR2,
                p_key       IN VARCHAR2,
                p_last_date IN VARCHAR2);
  PROCEDURE make_template(p_table  IN VARCHAR2,
                          p_result IN OUT NOCOPY CLOB);

END md_sync_server;
/
CREATE OR REPLACE PACKAGE BODY md_sync_server IS
  DATE_READ   CONSTANT VARCHAR2(30) := 'YYYYMMDD';
  DATE_FORMAT CONSTANT VARCHAR2(30) := 'YYYY-MM-DD"T"HH24:MI:SS';
  --===============================================================================
  PROCEDURE xmlStart IS
  BEGIN
    owa_util.mime_header('text/xml', bclose_header => FALSE);
    htp.p('Access-Control-Allow-Origin: *');
    owa_util.http_header_close;
    htp.p('<?xml version="1.0" encoding="utf-8"?>');
    htp.p('<result>');
  END;
  --===============================================================================
  PROCEDURE xmlEnd IS
  BEGIN
    htp.p('</result>');
  END;
  --===============================================================================
  PROCEDURE print(text IN OUT NOCOPY CLOB,
                  buf  IN CLOB) IS
  BEGIN
    text := text || buf || chr(10);
  END;
  --===============================================================================
  PROCEDURE xml(p_code      IN VARCHAR2,
                p_key       IN VARCHAR2,
                p_last_date IN VARCHAR2) IS
    l_table_name     md_sync_table.table_name%TYPE;
    l_table_date     md_sync_table.table_date%TYPE;
    l_table_template md_sync_table.template%TYPE;
  
    l_table_columns VARCHAR2(400);
    l_column        owa_util.ident_arr;
  
    TYPE empcurtyp IS REF CURSOR;
  
    c1      empcurtyp;
    l_sql   VARCHAR2(12000);
    curid   NUMBER;
    desctab dbms_sql.desc_tab2;
    colcnt  NUMBER;
  
    namevar VARCHAR2(4000 CHAR);
    numvar  NUMBER;
    datevar DATE;
  
    l_xml CLOB;
  BEGIN
    xmlStart;
    SELECT table_name, table_date, template
      INTO l_table_name, l_table_date, l_table_template
      FROM md_sync_table t
     WHERE t.code = p_code
       AND t.key = p_key
       AND t.date_delete IS NULL;
  
    FOR c IN (SELECT rtrim(ltrim(column_value, '{'), '}') column_value
                FROM TABLE(ax_regexp.match(l_table_template, '\{[A-Z]+\}'))) LOOP
      l_column(l_column.count + 1) := c.column_value;
      IF l_table_columns IS NOT NULL THEN
        l_table_columns := l_table_columns || ',';
      END IF;
      l_table_columns := l_table_columns || c.column_value;
    END LOOP;
  
    IF p_last_date IS NOT NULL THEN
      l_sql := 'SELECT ' || l_table_columns || ' FROM ' || l_table_name || ' t WHERE trunc(t.' || l_table_date ||
               ') >= to_date(''' || p_last_date || ''',''' || DATE_READ || ''')';
    ELSE
      l_sql := 'SELECT ' || l_table_columns || ' FROM ' || l_table_name || ' t';
    END IF;
  
    OPEN c1 FOR l_sql;
    curid := dbms_sql.to_cursor_number(c1);
    dbms_sql.describe_columns2(curid, colcnt, desctab);
    FOR i IN 1 .. colcnt LOOP
      IF desctab(i).col_type = 2 THEN
        dbms_sql.define_column(curid, i, numvar);
      ELSIF desctab(i).col_type = 12 THEN
        dbms_sql.define_column(curid, i, datevar);
      ELSE
        dbms_sql.define_column(curid, i, namevar, 4000);
      END IF;
    END LOOP;
  
    WHILE dbms_sql.fetch_rows(curid) > 0 LOOP
      FOR i IN 1 .. colcnt LOOP
        IF (desctab(i).col_type = 1) THEN
          dbms_sql.COLUMN_VALUE(curid, i, namevar);
          l_table_template := REPLACE(l_table_template, '{' || l_column(i) || '}', namevar);
          numvar           := NULL;
          datevar          := NULL;
        ELSIF (desctab(i).col_type = 2) THEN
          dbms_sql.COLUMN_VALUE(curid, i, numvar);
          l_table_template := REPLACE(l_table_template, '{' || l_column(i) || '}', numvar);
          namevar          := NULL;
          datevar          := NULL;
        ELSIF (desctab(i).col_type = 12) THEN
          dbms_sql.COLUMN_VALUE(curid, i, datevar);
          l_table_template := REPLACE(l_table_template, '{' || l_column(i) || '}', to_char(datevar, DATE_FORMAT));
          namevar          := NULL;
          numvar           := NULL;
        END IF;
      
      END LOOP;
      print(l_xml, '<ITEM>' || chr(10) || l_table_template || chr(10) || '</ITEM>');
    END LOOP;
  
    printclob(l_xml);
    xmlEnd;
  EXCEPTION
    WHEN OTHERS THEN
      htp.prn('<ERROR />');
      xmlEnd;
  END;
  --===============================================================================
  PROCEDURE make_template(p_table  IN VARCHAR2,
                          p_result IN OUT NOCOPY CLOB) IS
  BEGIN
    FOR c IN (SELECT t.COLUMN_NAME AS "RESULT"
                FROM user_tab_cols t
               WHERE t.TABLE_NAME = upper(p_table)
                 AND t.HIDDEN_COLUMN = 'NO'
               ORDER BY t.COLUMN_ID) LOOP
      p_result := p_result || '<' || c.result || '>{' || c.result || '}</' || c.result || '>' || chr(10);
    END LOOP;
    p_result := TRIM(p_result);
  END;
  --===============================================================================
END md_sync_server;
/
