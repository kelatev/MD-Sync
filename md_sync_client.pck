CREATE OR REPLACE PACKAGE md_sync_client IS

  -- Author  : KELATEV
  -- Created : 12.07.2017 13:18:08
  -- Purpose : 
  DATE_URL  CONSTANT VARCHAR2(30) := 'YYYYMMDD';
  DATE_DATA CONSTANT VARCHAR2(30 CHAR) := 'YYYY-MM-DD"T"HH24:MI:SS';

  DIC_LAWDOC       CONSTANT VARCHAR2(20) := 'lawdoc';
  DIC_LAWDOC_TEXT  CONSTANT VARCHAR2(20) := 'lawdoc_text';
  DIC_HS_2014      CONSTANT VARCHAR2(20) := 'hs_2014';
  DIC_HS_2014_TEXT CONSTANT VARCHAR2(20) := 'hs_2014_text';
  DIC_HS_CLASS     CONSTANT VARCHAR2(20) := 'hs_class';

  --PARSE
  PROCEDURE parce_document(p_code   IN VARCHAR2,
                           p_data   IN OUT NOCOPY CLOB,
                           p_offset IN OUT NOCOPY NUMBER,
                           p_limit  IN OUT NOCOPY NUMBER,
                           p_total  IN OUT NOCOPY NUMBER);
  PROCEDURE parse_doc_img;

  --ACTION
  PROCEDURE action_sync(p_code      IN VARCHAR2,
                        p_last_date IN DATE DEFAULT NULL,
                        p_offset    IN NUMBER DEFAULT 0);
  PROCEDURE action_sync_index(p_code IN VARCHAR2);

  --JOB
  PROCEDURE job_sync;

  PROCEDURE debug_on;
  PROCEDURE debug_off;

/** /
  UPDATE diction_hs_2014_text t SET t.doc_text = substr(t.doc_text
                                                       ,instr(t.doc_text, lower('</table>'), 1, 1) +
                                                        length('</table>'));
  /**/

END md_sync_client;
/
CREATE OR REPLACE PACKAGE BODY md_sync_client IS

  g_debug BOOLEAN := FALSE;

  SUBTYPE value_item IS VARCHAR2(128);
  TYPE value_list IS TABLE OF CLOB INDEX BY value_item;
  --===============================================================================
  PROCEDURE show_debug(p_env     IN VARCHAR2,
                       p_heading IN VARCHAR2 DEFAULT NULL) AS
    i     PLS_INTEGER;
    l_len PLS_INTEGER;
  BEGIN
    IF g_debug THEN
      IF p_heading IS NOT NULL THEN
        DBMS_OUTPUT.put_line('*****' || p_heading || '*****');
      END IF;
    
      i     := 1;
      l_len := LENGTH(p_env);
      WHILE (i <= l_len) LOOP
        DBMS_OUTPUT.put_line(SUBSTR(p_env, i, 80));
        i := i + 80;
      END LOOP;
    END IF;
  END;
  --===============================================================================
  --data_pk(p_table_name => , p_items => );
  PROCEDURE data_pk(p_table_name IN VARCHAR2,
                    p_items      IN OUT NOCOPY owa_util.vc_arr) IS
  BEGIN
    SELECT t.COLUMN_NAME
      BULK COLLECT
      INTO p_items
      FROM user_cons_columns t
     WHERE (t.OWNER, t.CONSTRAINT_NAME) = (SELECT t.OWNER, t.CONSTRAINT_NAME
                                             FROM user_constraints t
                                            WHERE t.TABLE_NAME = UPPER(p_table_name)
                                              AND t.CONSTRAINT_TYPE = 'P')
     ORDER BY t.POSITION;
  END;
  --===============================================================================
  --parse_item(p_table_name => , p_table_date => , p_pk_name => , p_value => );
  PROCEDURE parse_item(p_table_name IN OUT NOCOPY VARCHAR2,
                       p_table_date IN OUT NOCOPY VARCHAR2,
                       p_pk_name    IN OUT NOCOPY owa_util.vc_arr,
                       p_value      IN OUT NOCOPY value_list) IS
    TYPE clob_arr IS TABLE OF CLOB INDEX BY BINARY_INTEGER;
    l_clob_value clob_arr;
    l_clob_name  owa_util.ident_arr;
  
    l_varchar_value owa_util.vc_arr;
    l_varchar_name  owa_util.ident_arr;
  
    l_column_name user_tab_cols.column_name%TYPE;
    l_column_type user_tab_cols.data_type%TYPE;
  
    l_item_date DATE;
  
    PROCEDURE save_clob(p_clob_name  IN VARCHAR2,
                        p_clob_value IN CLOB) IS
    BEGIN
      EXECUTE IMMEDIATE 'update ' || p_table_name || ' set ' || p_clob_name || ' = :val where ' || p_pk_name(1) || '=' ||
                        p_value(p_pk_name(1))
        USING regexp_replace(p_clob_value, '</?a[^<>]*>', '');
      COMMIT;
    END;
  
    PROCEDURE save_varchar(p_clob_name  IN VARCHAR2,
                           p_clob_value IN VARCHAR2) IS
      l_chars VARCHAR2(32767 CHAR) := p_clob_value;
    BEGIN
      EXECUTE IMMEDIATE 'update ' || p_table_name || ' set ' || p_clob_name || ' = :val where ' || p_pk_name(1) || '=' ||
                        p_value(p_pk_name(1))
        USING regexp_replace(l_chars, '</?a[^<>]*>', '');
      COMMIT;
    END;
  BEGIN
    EXECUTE IMMEDIATE 'select ' || p_table_date || ' from ' || p_table_name || ' where ' || p_pk_name(1) || ' = ''' ||
                      p_value(p_pk_name(1)) || ''''
      INTO l_item_date;
    --dbms_output.put_line('exist');
    --IF (l_item_date <> l_param_value(l_date_name)) THEN
    --dbms_output.put_line('update');
    DECLARE
      l_update_statement VARCHAR2(32767 CHAR);
    BEGIN
      l_column_name := p_value.FIRST;
      WHILE l_column_name IS NOT NULL LOOP
        IF l_column_name = p_pk_name(1) THEN
          l_column_name := p_value.NEXT(l_column_name);
          CONTINUE;
        END IF;
      
        SELECT t.data_type
          INTO l_column_type
          FROM user_tab_cols t
         WHERE t.TABLE_NAME = upper(p_table_name)
           AND t.COLUMN_NAME = upper(l_column_name);
      
        l_update_statement := l_update_statement || ',' || l_column_name || '= ';
        IF p_value(l_column_name) IS NULL THEN
          l_update_statement := l_update_statement || 'NULL';
        ELSIF l_column_type = 'NUMBER' THEN
          l_update_statement := l_update_statement || p_value(l_column_name);
        ELSIF l_column_type = 'VARCHAR2' THEN
        
          IF length(p_value(l_column_name)) > 1000 THEN
            l_update_statement := l_update_statement || 'null';
          
            l_varchar_name(l_varchar_name.count + 1) := upper(l_column_name);
            l_varchar_value(l_varchar_value.count + 1) := util.xml_entity_decode(p_value(l_column_name));
          ELSE
            l_update_statement := l_update_statement || '''' || REPLACE(p_value(l_column_name), '''', '''''') || '''';
          END IF;
        
        ELSIF l_column_type = 'DATE' THEN
          l_update_statement := l_update_statement || 'to_date(''' || p_value(l_column_name) || ''',''' || DATE_DATA ||
                                ''')';
        ELSIF l_column_type = 'CLOB' THEN
          l_update_statement := l_update_statement || 'empty_clob()';
          l_clob_name(l_clob_name.count + 1) := upper(l_column_name);
          l_clob_value(l_clob_value.count + 1) := util.xml_entity_decode(p_value(l_column_name));
        END IF;
        l_column_name := p_value.NEXT(l_column_name);
      END LOOP;
      l_update_statement := ltrim(l_update_statement, ',');
    
      /*      dbms_output.put_line('update ' || p_table_name || ' set ' || l_update_statement || ' where ' || p_pk_name(1) || '=' ||
      p_value(p_pk_name(1)));*/
    
      EXECUTE IMMEDIATE 'update ' || p_table_name || ' set ' || l_update_statement || ' where ' || p_pk_name(1) || '=' ||
                        p_value(p_pk_name(1));
      COMMIT;
    END;
    --END IF;
  EXCEPTION
    WHEN NO_DATA_FOUND THEN
      --dbms_output.put_line('insert');
      DECLARE
        l_insert_name  VARCHAR2(32767 CHAR);
        l_insert_value VARCHAR2(32767 CHAR);
      BEGIN
        l_column_name := p_value.FIRST;
      
        WHILE l_column_name IS NOT NULL LOOP
          SELECT t.DATA_TYPE
            INTO l_column_type
            FROM user_tab_cols t
           WHERE t.TABLE_NAME = upper(p_table_name)
             AND t.COLUMN_NAME = upper(l_column_name);
        
          l_insert_name := l_insert_name || ',' || l_column_name;
          IF p_value(l_column_name) IS NULL THEN
            l_insert_value := l_insert_value || ',NULL';
          ELSIF l_column_type = 'NUMBER' THEN
            l_insert_value := l_insert_value || ',' || p_value(l_column_name);
          ELSIF l_column_type = 'VARCHAR2' THEN
            IF length(p_value(l_column_name)) > 1000 THEN
              l_insert_value := l_insert_value || ',null';
            
              l_varchar_name(l_varchar_name.count + 1) := upper(l_column_name);
              l_varchar_value(l_varchar_value.count + 1) := util.xml_entity_decode(p_value(l_column_name));
            ELSE
              l_insert_value := l_insert_value || ',''' || REPLACE(p_value(l_column_name), '''', '''''') || '''';
            END IF;
          ELSIF l_column_type = 'DATE' THEN
            l_insert_value := l_insert_value || ',to_date(''' || p_value(l_column_name) || ''',''' || DATE_DATA ||
                              ''')';
          ELSIF l_column_type = 'CLOB' THEN
            l_insert_value := l_insert_value || ',empty_clob()';
            l_clob_name(l_clob_name.count + 1) := upper(l_column_name);
            l_clob_value(l_clob_value.count + 1) := util.xml_entity_decode(p_value(l_column_name));
          END IF;
        
          l_column_name := p_value.NEXT(l_column_name);
        END LOOP;
        l_insert_name  := ltrim(l_insert_name, ',');
        l_insert_value := ltrim(l_insert_value, ',');
      
        --dbms_output.put_line('insert into ' || p_table_name || '(' || l_insert_name || ') VALUES(' || l_insert_value || ')');
        --show_debug(l_insert_name, 'l_insert_name');
        --show_debug(l_insert_value, 'l_insert_value');
        EXECUTE IMMEDIATE 'insert into ' || p_table_name || '(' || l_insert_name || ') VALUES(' || l_insert_value || ')';
        COMMIT;
      END;
      FOR i IN 1 .. l_varchar_name.count LOOP
        dbms_output.put_line('length(l_varchar_value(i)) >> ' || length(l_varchar_value(i)));
        save_varchar(p_clob_name => l_varchar_name(i), p_clob_value => l_varchar_value(i));
      END LOOP;
    
      FOR i IN 1 .. l_clob_name.count LOOP
        save_clob(p_clob_name => l_clob_name(i), p_clob_value => l_clob_value(i));
      END LOOP;
  END;
  --===============================================================================
  PROCEDURE parce_document(p_code   IN VARCHAR2,
                           p_data   IN OUT NOCOPY CLOB,
                           p_offset IN OUT NOCOPY NUMBER,
                           p_limit  IN OUT NOCOPY NUMBER,
                           p_total  IN OUT NOCOPY NUMBER) IS
    l_table_name md_sync_table.table_name%TYPE;
    l_date_name  md_sync_table.Table_date%TYPE;
  
    l_pk_names owa_util.vc_arr;
  
    l_xml xmltype;
  
    l_count NUMBER;
    l_doc   dbms_xmldom.DOMDocument;
    l_node  dbms_xmldom.DOMNode;
  
    l_list         dbms_xmldom.DOMNodeList;
    l_child_list   dbms_xmldom.DOMNodeList;
    l_child_counts NUMBER;
    l_child        dbms_xmldom.DOMNode;
  
    l_param_value value_list;
    l_param_temp  CLOB;
  
  BEGIN
    SELECT t.table_name, t.table_date INTO l_table_name, l_date_name FROM md_sync_table t WHERE t.code = p_code;
  
    data_pk(p_table_name => l_table_name, p_items => l_pk_names);
  
    l_xml := XMLTYPE.createXML(p_data);
  
    l_doc   := dbms_xmldom.newdomdocument(l_xml);
    l_list  := dbms_xmldom.getChildNodes(dbms_xmldom.getFirstChild(dbms_xmldom.MakeNode(l_doc)));
    l_count := dbms_xmldom.getLength(l_list);
  
    FOR i IN 0 .. l_count - 1 LOOP
      l_node := dbms_xmldom.item(l_list, i);
      --dbms_output.put_line(dbms_xmldom.getNodeName(l_node));
      --CONTINUE;
      CASE dbms_xmldom.getNodeName(l_node)
        WHEN 'OFFSET' THEN
          --dbms_output.put_line('%' || dbms_xmldom.getNodeValue(dbms_xmldom.getfirstchild(l_node)) || '%');
          p_offset := dbms_xmldom.getNodeValue(dbms_xmldom.getfirstchild(l_node));
        WHEN 'LIMIT' THEN
          p_limit := dbms_xmldom.getNodeValue(dbms_xmldom.getfirstchild(l_node));
        WHEN 'TOTAL' THEN
          p_total := dbms_xmldom.getNodeValue(dbms_xmldom.getfirstchild(l_node));
        ELSE
          l_child_list   := dbms_xmldom.getChildNodes(l_node);
          l_child_counts := dbms_xmldom.getLength(l_child_list);
          --    dbms_output.put_line('l_child_nodes_count >> ' || l_child_counts);
          FOR j IN 0 .. l_child_counts - 1 LOOP
            l_child := dbms_xmldom.item(l_child_list, j);
            --dbms_output.put_line('type1=' || dbms_xmldom.getNodeType(dbms_xmldom.getfirstchild(l_child)));
            --      dbms_output.put_line('name1=' || dbms_xmldom.getNodeName(l_child));
            --      dbms_output.put_line('value1=' || dbms_xmldom.getNodeValue(dbms_xmldom.getfirstchild(l_child)));   
          
            DBMS_LOB.CreateTemporary(l_param_temp, TRUE);
            DBMS_XMLDOM.WRITETOCLOB(dbms_xmldom.getfirstchild(l_child), l_param_temp);
            /*        l_param_value(dbms_xmldom.getNodeName(l_child)) := dbms_xmldom.getNodeValue(dbms_xmldom.getfirstchild(l_child));*/
            l_param_value(dbms_xmldom.getNodeName(l_child)) := util.xml_entity_decode(l_param_temp);
            --dbms_lob.freetemporary(l_param_temp);
          END LOOP;
        
          parse_item(p_table_name => l_table_name,
                     p_table_date => l_date_name,
                     p_pk_name    => l_pk_names,
                     p_value      => l_param_value);
      END CASE;
    END LOOP;
    dbms_xmldom.freeDocument(l_doc);
  END;
  --=============================================================================== 
  -- Created on 22.08.2017 by MDUSER 
  PROCEDURE parse_doc_img IS
    pattern VARCHAR2(100) := '<img SRC="(.*?)"(.*?)>';
    --path_pattern      VARCHAR2(32767) := '<img SRC="(.*?)">';
    old_file_path     VARCHAR2(32767);
    dirName           VARCHAR2(32767) := 'PORTAL_IMAGE_DOC';
    fileName          VARCHAR2(32767);
    l_text            diction_hs_2014_text.doc_text%TYPE;
    l_blob            BLOB;
    item_to_update_id NUMBER;
  
    tmp VARCHAR2(32767);
  BEGIN
    FOR c IN (SELECT *
                FROM (SELECT t.hs_id, t.doc_text
                        FROM diction_hs_2014_text t
                       WHERE (CONTAINS(t.doc_text, '(img src\="/images/) NOT (img src\="/images/doc/)', 1) > 0))
               WHERE rownum < 200) LOOP
      l_text := c.doc_text;
      --dbms_output.put_line('c.hs_id >> ' || c.hs_id);
      FOR c2 IN (SELECT column_value
                   FROM TABLE(ax_regexp.match(p_str => c.doc_text, p_pattern => pattern, modifier => 'i'))
                  WHERE column_value NOT LIKE ('%/images/doc/%')) LOOP
        --get path to img
        --dbms_output.put_line('c2.column_value >> ' || c2.column_value);
        /*old_file_path := regexp_replace(c2.column_value, pattern, '\1');*/
        old_file_path := regexp_replace(c2.column_value, pattern, '\1', modifier => 'i');
        --dbms_output.put_line('old_file_path >> ' || old_file_path);
        --dowload image save image to file
        fileName := ax_file.extract_filename(p_file_name => old_file_path, p_os => ax_file.g_os_unix);
        --dbms_output.put_line('fileName >> ' || fileName);
        IF NOT ax_file.file_exists(p_directory_name => dirName, p_file_name => fileName) THEN
          l_blob := ax_http.get_blob_from_url(p_url => 'www.mdoffice.com.ua' || old_file_path);
          ax_file.save_blob_to_file(p_directory_name => dirName, p_file_name => fileName, p_blob => l_blob);
        END IF;
        /*        dbms_output.put_line('c2.column_value ater replace >> ' ||
        REPLACE(c2.column_value, old_file_path, '/images/doc/' || fileName));*/
        --replace path in doc
        l_text := REPLACE(l_text, c2.column_value, REPLACE(c2.column_value, old_file_path, '/images/doc/' || fileName));
      END LOOP;
      item_to_update_id := c.hs_id;
      UPDATE diction_hs_2014_text r SET r.doc_text = l_text WHERE r.hs_id = item_to_update_id;
      COMMIT;
    END LOOP;
  END;
  --===============================================================================
  FUNCTION make_uri(p_code      IN VARCHAR2,
                    p_last_date IN DATE DEFAULT SYSDATE,
                    p_offset    IN NUMBER) RETURN VARCHAR2 IS
    l_url VARCHAR2(1000);
  BEGIN
    SELECT REPLACE(REPLACE(REPLACE(REPLACE(t.url, '{CODE}', p_code), '{KEY}', t.key),
                           '{DATE}',
                           to_char(p_last_date, DATE_URL)),
                   '{OFFSET}',
                   p_offset)
      INTO l_url
      FROM md_sync_table t
     WHERE t.code = p_code;
  
    show_debug(l_url, 'Url');
  
    RETURN l_url;
  END;
  --===============================================================================
  PROCEDURE save_responce(p_code      IN VARCHAR2,
                          p_last_date IN DATE,
                          p_responce  IN OUT NOCOPY CLOB) IS
  BEGIN
    INSERT INTO md_sync_response (md_sync_code, responce, last_date) VALUES (p_code, p_responce, p_last_date);
    COMMIT;
  
    UPDATE md_sync_table SET last_date = SYSDATE WHERE code = p_code;
    COMMIT;
  END;
  --===============================================================================
  PROCEDURE make_job(p_code      IN VARCHAR2,
                     p_last_date IN DATE,
                     p_offset    IN NUMBER) IS
    l_next_time FLOAT := 1 / 1440; --in day
    l_job_name  VARCHAR2(100);
  BEGIN
    l_job_name := 'TPPADMIN.DIC_SYNC_' || p_code || '_' || p_offset;
  
    sys.dbms_scheduler.create_job(job_name            => l_job_name,
                                  job_type            => 'STORED_PROCEDURE',
                                  job_action          => 'tppadmin.md_sync_client.action_sync',
                                  start_date          => SYSDATE + l_next_time,
                                  job_class           => 'DEFAULT_JOB_CLASS',
                                  number_of_arguments => 3,
                                  enabled             => FALSE,
                                  auto_drop           => TRUE);
    sys.dbms_scheduler.set_job_argument_value(job_name => l_job_name, argument_position => 1, argument_value => p_code);
    sys.dbms_scheduler.set_job_argument_value(job_name          => l_job_name,
                                              argument_position => 2,
                                              argument_value    => p_last_date);
    sys.dbms_scheduler.set_job_argument_value(job_name          => l_job_name,
                                              argument_position => 3,
                                              argument_value    => p_offset);
    sys.dbms_scheduler.enable(name => l_job_name);
  END;
  --===============================================================================
  PROCEDURE action_sync(p_code      IN VARCHAR2,
                        p_last_date IN DATE DEFAULT NULL,
                        p_offset    IN NUMBER DEFAULT 0) IS
    l_url         VARCHAR2(400);
    l_param_name  owa_util.vc_arr;
    l_param_value owa_util.vc_arr;
  
    l_result CLOB;
  
    l_resp_offset NUMBER;
    l_resp_limit  NUMBER;
    l_resp_total  NUMBER;
  BEGIN
    l_url := make_uri(p_code => p_code, p_last_date => p_last_date, p_offset => p_offset);
    l_param_name(1) := 'Content-type';
    l_param_value(1) := 'text/json; charset=windows-1251';
    l_result := ax_http.get_clob_from_url(p_url => l_url, p_param_name => l_param_name, p_param_value => l_param_value);
    save_responce(p_code => p_code, p_last_date => p_last_date, p_responce => l_result);
  
    parce_document(p_code   => p_code,
                   p_data   => l_result,
                   p_offset => l_resp_offset,
                   p_limit  => l_resp_limit,
                   p_total  => l_resp_total);
  
    IF l_resp_total IS NOT NULL
       AND l_resp_limit IS NOT NULL THEN
      IF l_resp_total > p_offset + l_resp_limit
         AND p_offset < 200000
         AND l_resp_limit > 0 THEN
        make_job(p_code => p_code, p_last_date => p_last_date, p_offset => p_offset + l_resp_limit);
      ELSE
        --parse_doc_img(p_code => p_code);
        action_sync_index(p_code => p_code);
      END IF;
    ELSE
      sa_logs.err(i_routine_nm => 'md_sync_client', i_with_info => TRUE);
    END IF;
  END;
  --===============================================================================
  PROCEDURE action_sync_index(p_code IN VARCHAR2) IS
  BEGIN
    FOR c IN (SELECT i.index_name
                FROM user_indexes i, md_sync_table s
               WHERE s.code = p_code
                 AND upper(s.table_name) = i.TABLE_NAME
                 AND i.ITYP_NAME = 'CONTEXT'
                 AND i.ITYP_OWNER = 'CTXSYS') LOOP
      CTX_DDL.SYNC_INDEX(idx_name => c.index_name);
    END LOOP;
  END;
  --===============================================================================
  PROCEDURE job_sync IS
  BEGIN
    FOR c IN (SELECT code, last_date, table_name
                FROM md_sync_table
               WHERE (last_date + 30 < SYSDATE OR last_date IS NULL)) LOOP
      show_debug(c.code, 'Job_sync - code');
      show_debug(c.last_date, 'Job_sync - last_date');
    
      action_sync(p_code => c.code, p_last_date => c.last_date);
    END LOOP;
  END;
  --===============================================================================
  PROCEDURE debug_on AS
  BEGIN
    g_debug := TRUE;
  END;
  --===============================================================================
  PROCEDURE debug_off AS
  BEGIN
    g_debug := FALSE;
  END;
  --===============================================================================
END md_sync_client;
/
