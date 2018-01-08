CREATE OR REPLACE PACKAGE md_sync_server IS

  -- Author  : KELATEV
  -- Created : 14.07.2017 10:16:53
  -- Purpose : 

  PROCEDURE xml(p_code      IN VARCHAR2,
                p_key       IN VARCHAR2,
                p_last_date IN VARCHAR2,
                p_offset    IN NUMBER DEFAULT 0);

  FUNCTION authorize RETURN BOOLEAN;

END md_sync_server;
/
CREATE OR REPLACE PACKAGE BODY md_sync_server IS
  DATE_READ    CONSTANT VARCHAR2(30) := 'YYYYMMDD';
  DATE_FORMAT  CONSTANT VARCHAR2(30) := 'YYYY-MM-DD"T"HH24:MI:SS';
  RESULT_LIMIT CONSTANT NUMBER := 300;

  TYPE t_table IS TABLE OF dbms_sql.clob_table INDEX BY BINARY_INTEGER;
  --===============================================================================
  FUNCTION clob_to_blob(p_clob CLOB) RETURN BLOB AS
    l_blob          BLOB;
    l_dest_offset   INTEGER := 1;
    l_source_offset INTEGER := 1;
    l_lang_context  INTEGER := DBMS_LOB.DEFAULT_LANG_CTX;
    l_warning       INTEGER := DBMS_LOB.WARN_INCONVERTIBLE_CHAR;
  BEGIN
  
    DBMS_LOB.CREATETEMPORARY(l_blob, TRUE);
    DBMS_LOB.CONVERTTOBLOB(dest_lob     => l_blob,
                           src_clob     => p_clob,
                           amount       => DBMS_LOB.LOBMAXSIZE,
                           dest_offset  => l_dest_offset,
                           src_offset   => l_source_offset,
                           blob_csid    => DBMS_LOB.DEFAULT_CSID,
                           lang_context => l_lang_context,
                           warning      => l_warning);
    RETURN l_blob;
  END;
  --===============================================================================
  FUNCTION encode_base64(p_blob_in IN BLOB) RETURN CLOB IS
    v_clob           CLOB;
    v_result         CLOB;
    v_offset         INTEGER;
    v_chunk_size     BINARY_INTEGER := (48 / 4) * 3;
    v_buffer_varchar VARCHAR2(48);
    v_buffer_raw     RAW(48);
  BEGIN
    IF p_blob_in IS NULL THEN
      RETURN NULL;
    END IF;
    dbms_lob.createtemporary(v_clob, TRUE);
    v_offset := 1;
    FOR i IN 1 .. ceil(dbms_lob.getlength(p_blob_in) / v_chunk_size) LOOP
      dbms_lob.read(p_blob_in, v_chunk_size, v_offset, v_buffer_raw);
      v_buffer_raw     := utl_encode.base64_encode(v_buffer_raw);
      v_buffer_varchar := utl_raw.cast_to_varchar2(v_buffer_raw);
      dbms_lob.writeappend(v_clob, length(v_buffer_varchar), v_buffer_varchar);
      v_offset := v_offset + v_chunk_size;
    END LOOP;
    v_result := v_clob;
    dbms_lob.freetemporary(v_clob);
    RETURN v_result;
  END;
  --===============================================================================
  FUNCTION Encode(p_clob CLOB) RETURN CLOB IS
    l_clob   CLOB;
    l_len    NUMBER;
    l_pos    NUMBER := 1;
    l_buffer VARCHAR2(32767);
    l_amount NUMBER := 32767;
  BEGIN
    l_len := dbms_lob.getlength(p_clob);
    dbms_lob.createtemporary(l_clob, TRUE);
  
    WHILE l_pos <= l_len LOOP
      dbms_lob.read(p_clob, l_amount, l_pos, l_buffer);
      l_buffer := utl_encode.text_encode(l_buffer, encoding => utl_encode.base64);
      l_pos    := l_pos + l_amount;
      dbms_lob.writeappend(l_clob, length(l_buffer), l_buffer);
    END LOOP;
  
    RETURN l_clob;
  END;
  --===============================================================================
  FUNCTION encode_base64_2(p_blob_in IN BLOB) RETURN CLOB IS
    l_step PLS_INTEGER := 12000; -- make sure you set a multiple of 3 not higher than 24573
  
    l_raw    RAW(12000);
    l_clob   CLOB;
    l_result CLOB;
  BEGIN
    FOR i IN 0 .. TRUNC((DBMS_LOB.getlength(p_blob_in) - 1) / l_step) LOOP
      l_raw    := DBMS_LOB.substr(p_blob_in, l_step, i * l_step + 1);
      l_raw    := UTL_ENCODE.base64_encode(l_raw);
      l_result := l_result || UTL_RAW.cast_to_varchar2(l_raw);
    END LOOP;
    RETURN l_result;
  END;
  --===============================================================================
  PROCEDURE xmlStart IS
  BEGIN
    owa_util.mime_header('text/xml', bclose_header => FALSE);
    htp.p('Access-Control-Allow-Origin: *');
    owa_util.http_header_close;
    htp.p('<?xml version="1.0" encoding="windows-1251"?>');
    htp.p('<result>');
  END;
  --===============================================================================
  PROCEDURE xmlEnd IS
  BEGIN
    htp.p('</result>');
  END;
  --===============================================================================
  PROCEDURE xmlPrint(text IN OUT NOCOPY CLOB,
                     buf  IN VARCHAR2) IS
  BEGIN
    text := text || buf || chr(10);
  END;
  --===============================================================================
  PROCEDURE xmlPrint(text IN OUT NOCOPY CLOB,
                     buf  IN CLOB) IS
  BEGIN
    text := text || buf || chr(10);
  END;
  --===============================================================================
  PROCEDURE get_data(p_sql         IN VARCHAR2,
                     p_offset      IN NUMBER,
                     p_limit       IN NUMBER,
                     p_item_column IN OUT NOCOPY dbms_sql.varchar2_table,
                     p_item_list   IN OUT NOCOPY t_table,
                     p_item_total  IN OUT NOCOPY NUMBER) IS
    l_query  VARCHAR2(32000);
    l_cursor INTEGER;
  
    desctab dbms_sql.desc_tab3;
    colcnt  NUMBER;
  
    namevar   VARCHAR2(4000 CHAR);
    numvar    NUMBER;
    datevar   DATE;
    clobvar   CLOB;
    l_execute INTEGER;
  
    l_i NUMBER := 1;
  BEGIN
    EXECUTE IMMEDIATE 'select count(*) from ( ' || p_sql || ' )'
      INTO p_item_total;
  
    l_query  := 'select /*+ FIRST_ROWS(' || to_char(p_limit) || ') */ t.* from 
  ( ' || p_sql || ' )t  OFFSET ' || p_offset || ' ROWS FETCH NEXT ' || p_limit || ' ROWS ONLY';
    l_cursor := dbms_sql.open_cursor;
    dbms_sql.parse(l_cursor, l_query, dbms_sql.native);
  
    dbms_sql.describe_columns3(l_cursor, colcnt, desctab);
    FOR i IN 1 .. colcnt LOOP
      IF desctab(i).col_type = 2 THEN
        dbms_sql.define_column(l_cursor, i, numvar);
      ELSIF desctab(i).col_type = 12 THEN
        dbms_sql.define_column(l_cursor, i, datevar);
      ELSIF (desctab(i).col_type = 112) THEN
        dbms_sql.define_column(l_cursor, i, clobvar);
      ELSE
        dbms_sql.define_column(l_cursor, i, namevar, 4000);
      END IF;
      p_item_column(i) := desctab(i).col_name;
    END LOOP;
  
    l_execute := dbms_sql.execute(l_cursor);
    WHILE dbms_sql.fetch_rows(l_cursor) > 0 LOOP
      DECLARE
        l_row dbms_sql.Clob_Table;
        l_j   NUMBER := 1;
      BEGIN
        FOR i IN 1 .. colcnt LOOP
          IF desctab(i).col_type = 1 THEN
            dbms_sql.COLUMN_VALUE(l_cursor, i, namevar);
            l_row(l_j) := namevar;
          ELSIF (desctab(i).col_type = 2) THEN
            dbms_sql.COLUMN_VALUE(l_cursor, i, numvar);
            l_row(l_j) := to_char(numvar);
          ELSIF (desctab(i).col_type = 12) THEN
            dbms_sql.COLUMN_VALUE(l_cursor, i, datevar);
            l_row(l_j) := to_char(datevar, DATE_FORMAT);
          ELSIF (desctab(i).col_type = 112) THEN
            dbms_sql.COLUMN_VALUE(l_cursor, i, clobvar);
            l_row(l_j) := clobvar;
          END IF;
        
          l_j := l_j + 1;
        END LOOP;
      
        p_item_list(l_i) := l_row;
        l_i := l_i + 1;
      END;
    END LOOP;
  
    dbms_sql.close_cursor(l_cursor);
  END;
  --===============================================================================
  PROCEDURE make_info_xml(p_xml    IN OUT NOCOPY CLOB,
                          p_offset IN NUMBER,
                          p_limit  IN NUMBER,
                          p_total  IN NUMBER) IS
  BEGIN
    xmlPrint(p_xml, '<OFFSET>' || p_offset || '</OFFSET>');
    xmlPrint(p_xml, '<LIMIT>' || p_limit || '</LIMIT>');
    xmlPrint(p_xml, '<TOTAL>' || p_total || '</TOTAL>');
  END;
  --===============================================================================
  PROCEDURE make_item_xml(p_xml         IN OUT NOCOPY CLOB,
                          p_item_column IN OUT NOCOPY dbms_sql.varchar2_table,
                          p_item_list   IN OUT NOCOPY t_table) IS
    l_xml CLOB;
    l_zip BLOB;
  BEGIN
    FOR i IN 1 .. p_item_list.count LOOP
      DECLARE
        l_row     dbms_sql.clob_table := p_item_list(i);
        l_row_xml CLOB;
      BEGIN
        FOR j IN 1 .. l_row.count LOOP
          DECLARE
            l_title VARCHAR2(4000) := p_item_column(j);
          BEGIN
            /*IF (desctab(i).col_type = 112) THEN
              l_item := l_item || '<' || desctab(i).col_name || '><![CDATA[' || ahtml.xml_entity_encode(l_value, TRUE) ||
                        ']]></' || desctab(i).col_name || '>' || chr(10);
            ELSE*/
            l_row_xml := l_row_xml || '<' || l_title || '>' || ahtml.xml_entity_encode(l_row(j)) || '</' || l_title || '>' ||
                         chr(10);
            --END IF;
          END;
        END LOOP;
        l_xml := l_xml || '<ITEM>' || l_row_xml || '</ITEM>' || chr(10);
        --xmlPrint(p_xml, '<ITEM>' || chr(10) || l_row_xml || '</ITEM>');
      END;
    END LOOP;
  
    l_zip := clob_to_blob(l_xml);
    --l_zip := UTL_COMPRESS.lz_compress(l_zip);
    --as_zip.add1file(l_zip, 'doc.xml', l_zip);
    --as_zip.finish_zip(l_zip);
    l_xml := encode_base64_2(l_zip);
    xmlPrint(p_xml, '<DATA><![CDATA[' || l_xml || ']]></DATA>');
  END;
  --===============================================================================
  PROCEDURE xml(p_code      IN VARCHAR2,
                p_key       IN VARCHAR2,
                p_last_date IN VARCHAR2,
                p_offset    IN NUMBER DEFAULT 0) IS
    l_table_date md_sync_table.table_date%TYPE;
    l_sql        VARCHAR2(4000);
  
    l_item_column dbms_sql.varchar2_table;
    l_item_list   t_table;
    l_item_total  NUMBER;
  
    l_xml CLOB := '';
  BEGIN
    xmlStart;
    SELECT table_date, server_sql
      INTO l_table_date, l_sql
      FROM md_sync_table t
     WHERE t.code = p_code
       AND t.key = p_key
       AND t.date_delete IS NULL;
  
    l_sql := 'select * from (' || l_sql || ') t';
    IF p_last_date IS NOT NULL THEN
      l_sql := l_sql || ' WHERE trunc(t.' || l_table_date || ') >= to_date(''' || p_last_date || ''',''' || DATE_READ ||
               ''')';
    END IF;
  
    get_data(p_sql         => l_sql,
             p_offset      => p_offset,
             p_limit       => RESULT_LIMIT,
             p_item_column => l_item_column,
             p_item_list   => l_item_list,
             p_item_total  => l_item_total);
  
    make_info_xml(p_xml => l_xml, p_offset => p_offset, p_limit => RESULT_LIMIT, p_total => l_item_total);
    make_item_xml(p_xml => l_xml, p_item_column => l_item_column, p_item_list => l_item_list);
  
    printclob(l_xml);
    xmlEnd;
  EXCEPTION
    WHEN OTHERS THEN
      htp.prn('<ERROR message="' || SQLERRM || '"/>');
      xmlEnd;
  END;
  --===============================================================================
  FUNCTION authorize RETURN BOOLEAN IS
  BEGIN
    RETURN TRUE;
  END;
BEGIN
  owa_sec.set_authorization(OWA_SEC.PER_PACKAGE);
  --===============================================================================
END md_sync_server;
/
