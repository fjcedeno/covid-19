############CONEXION HANA#####################

hana_get_complete_table<-function(jdbcConnection,nombre)
{
  queryHana<-gsub('@table',nombre,"SELECT DISTINCT * FROM CHI_TXD_FRONT.FC_@table")
  dataTiendas= data.table::setDT(RJDBC::dbGetQuery(jdbcConnection, queryHana))
  return(dataTiendas)
}
hana_get_complete_table_week<-function(jdbcConnection,nombre)
{
  queryHana<-gsub('@table',nombre,"SELECT *, ADD_DAYS ( TO_DATE (FECHA, 'YYYY-MM-DD'), ( WEEKDAY( FECHA ) * -1 ) ) AS SEMANA FROM CHI_TXD_FRONT.FC_@table")
  dataTiendas= data.table::setDT(RJDBC::dbGetQuery(jdbcConnection, queryHana))
  return(dataTiendas)
}

hana_get_complete_table_externa_week<-function(jdbcConnection,nombre)
{
  queryHana<-gsub('@table',nombre,"SELECT *, ADD_DAYS ( TO_DATE (FECHA, 'YYYY-MM-DD'), ( WEEKDAY( FECHA ) * -1 ) ) AS SEMANA FROM CHI_TXD_FRONT.@table")
  dataTiendas= data.table::setDT(RJDBC::dbGetQuery(jdbcConnection, queryHana))
  return(dataTiendas)
}


hana_drop_table<-function(jdbcConnection,nombre)
{
  
  nombretabla<-gsub('@nombre',nombre,"FC_@nombre")
  if (DBI::dbExistsTable(jdbcConnection, nombretabla))
  {
    query<-gsub('@query',nombretabla,'CHI_TXD_FRONT.@query')
    DBI::dbRemoveTable(jdbcConnection,query)
  }
  return()
}


hana_write_table<-function(jdbcConnection,df,nombre)
{
  query<-gsub('@nombre',nombre,'CHI_TXD_FRONT.FC_@nombre')
  DBI::dbWriteTable(jdbcConnection,query,df)
}


hana_insert_table<-function(jdbcConnection,tablain,tablafin)
{
  
  queryHana<-"INSERT INTO CHI_TXD_FRONT.FC_@tablafin (
  SELECT * FROM CHI_TXD_FRONT.FC_@tablain
  )"
  
  queryHana<-gsub('@tablain',tablain,queryHana)
  queryHana<-gsub('@tablafin',tablafin,queryHana)
  
  out <- tryCatch(
    {
      DBI::dbSendQuery(jdbcConnection,queryHana)
    },
    error=function(cond) {
      
    },
    warning=function(cond) {
      
    },
    finally={
      
    }
  )
  
  return(out)
  
}

hana_truncate_table<-function(jdbcConnection,nombre)
{
  nombretabla<-gsub('@table',nombre,'CHI_TXD_FRONT.FC_@table')
  queryHana<-gsub('@table',nombretabla,"TRUNCATE TABLE @table")
  
  out <- tryCatch(
    {
      RJDBC::dbSendQuery(jdbcConnection,queryHana)
    },
    error=function(cond) {
      
    },
    warning=function(cond) {
      
    },
    finally={
      
    }
  )
  
  return(out)
  
}


hana_public_table<-function(jdbcConnection,tablain)
{
  
  queryHana<-"GRANT SELECT ON PQ0.CHI_TXD_FRONT.FC_@tablain TO PUBLIC"
  queryHana<-gsub('@tablain',tablain,queryHana)
  out <- tryCatch(
    {
      DBI::dbSendQuery(jdbcConnection,queryHana)
    },
    error=function(cond) {
      
    },
    warning=function(cond) {
      
    },
    finally={
      
    }
  )
  
  return(out)
  
}

