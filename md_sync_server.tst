PL/SQL Developer Test script 3.0
21
-- Created on 14.07.2017 by KELATEV 
DECLARE
  l_access_code    md_sync_table.code%TYPE := dbms_random.string('l', 20);
  l_access_key     md_sync_table.key%TYPE := dbms_random.string('a', 20);
  l_table_name     md_sync_table.table_name%TYPE := 'DICTION_COUNTRY_GROUP';
  l_table_date     md_sync_table.table_date%TYPE := 'DATE_MODIFY';
  l_table_template md_sync_table.template%TYPE;
BEGIN
  -- Test statements here
  md_sync_server.make_template(p_table => l_table_name, p_result => l_table_template);
  INSERT INTO md_sync_table
    (code, key, table_name, template, table_date)
  VALUES
    (l_access_code, l_access_key, l_table_name, l_table_template, l_table_date);
  COMMIT;
  
  --md_sync_server.xml

  DELETE md_sync_table WHERE code = l_access_code;
  COMMIT;
END;
0
0
