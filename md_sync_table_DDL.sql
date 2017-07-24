------------------------------------------------
-- Export file for user TPPADMIN@UCCI         --
-- Created by Kelatev on 12.07.2017, 14:20:03 --
------------------------------------------------

set define off
spool DDL.log

prompt
prompt Creating table MD_SYNC_TABLE
prompt ============================
prompt
create table MD_SYNC_TABLE
(
  code        VARCHAR2(100) not null,
  key         VARCHAR2(200),
  table_name  VARCHAR2(32),
  template    CLOB,
  last_date   DATE,
  date_create DATE,
  date_modify DATE,
  date_delete DATE
)
;
alter table MD_SYNC_TABLE
  add constraint PK_MD_SYNC_TABLE primary key (CODE);


spool off
