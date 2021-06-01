#0. SET WORKING DIRECTORY,CLEAN SCREEN, REMOVE VARIABLES----
setwd('~/covid-19')
cat("\014") 
graphics.off()
rm(list = ls())
options(java.parameters = "-Xmx8g")
options(encoding = 'UTF-8')
#install.packages("fMarkovSwitching", repos="http://R-Forge.R-project.org")
library(httr)
set_config(config(ssl_verifypeer = 0L))
library(dplyr)
library(data.table)
library(ggplot2)
library(scales)
library(forecast)
library(reshape)
#library(fMarkovSwitching)
#source("helper_scraper.R", encoding = "latin1", local = TRUE)
eval(parse('helper_scraper.R', encoding = 'UTF-8'))
source("helper_hana.R", encoding = "UTF-8", local = TRUE)
source("funciones.R", encoding = "UTF-8", local = TRUE)
source('~/connections/connections.R', encoding = "UTF-8", local = TRUE)
#Load Xlsx
emolRegion<-openxlsx::read.xlsx("~/PROYECTO_ALLOCATION/xlsx/minSalRegiones.xlsx")
start_time=Sys.time()
print(start_time)
hanaConnection<-hana_connect()

actualizar=TRUE
descargar=TRUE
borrar=TRUE
EMOL=TRUE
marcoTotal<-data.table::setDT(openxlsx::read.xlsx("./xlsx/divisionPoliticoTerritorial.xlsx") )

#1.	DESCARGAR DATA DEL MIMISTERIO DE SALUD ----
dir.create("~/covid-19/data",showWarnings = FALSE)
if(descargar)
{
  html<-"https://github.com/MinCiencia/Datos-COVID19"
  productos<-scraper_files_product(html)
  productos<-unique(productos)
  productos<-productos[c(1,2,15,41,4,7,52,54,18,65,25,13,19,26,27,45,74)]
  descripcion_productos<-scraper_files_desc(html)
  descripcion<-data.table::data.table(descripcion_productos=descripcion_productos)
  descripcion[,producto:=unlist(strsplit(descripcion_productos,"-"))[1],by=seq_len(nrow(descripcion))]
  data.table::fwrite(descripcion,file ="~/covid-19/diccionario/desc_producto.csv" ,sep=";",dec=",")
  
  for(i in 1:length(productos))
  {
    assign(productos[i], funciones_download_csv_github(product=productos[i]))
    covid.file<-get(productos[i])
    files<-names(covid.file)
    if(length(files)>0)
    {
      for(j in files)
      {
        data.table::fwrite(covid.file[[j]],file = paste0("./data/",productos[i],"_",j),sep=";",dec=",")
      } 
    }
    
    
    
    
  }  
}
# 2. ORDENAR LA DATA ----

CasosActualesPorComuna<-data.table::fread("./data/producto1_Covid-19_std.csv",sep=";",dec=",")
data.table::setnames(CasosActualesPorComuna,c("Codigo comuna","Casos confirmados"),c("codigo_comuna","casos"),skip_absent = TRUE)
CasosActualesPorComuna<-CasosActualesPorComuna[,c('codigo_comuna','Poblacion','Fecha','casos'),with=FALSE]
CasosActualesPorComuna<-na.omit(CasosActualesPorComuna)
CasosActualesPorComuna[,Fecha:=as.Date(Fecha)]

if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_CASOS_NUEVO_COMUNA"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_CASOS_NUEVO_COMUNA")
    hana_write_table(jdbcConnection = hanaConnection,df=CasosActualesPorComuna,nombre = "COVID_CASOS_NUEVO_COMUNA")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=CasosActualesPorComuna,nombre = "COVID_CASOS_NUEVO_COMUNA")
  }
  
}



# 2.1 casos nuevos por comuna  ----

listfiles<-list.files("./data",patter="producto2_",full.names = TRUE)
CasosNuevosPorComuna<-base::lapply(listfiles,data.table::fread)
CasosNuevosPorComuna<-data.table::rbindlist(CasosNuevosPorComuna)
CasosNuevosPorComuna[,fecha:=paste0(head(unlist(base::strsplit(name,split="-")),3),collapse = "-"),by=seq_len(nrow(CasosNuevosPorComuna))]
CasosNuevosPorComuna[,name:=NULL]
data.table::setnames(CasosNuevosPorComuna,c("Codigo comuna","Casos Confirmados"),c("codigo_comuna","casos_confirmado_comuna"),skip_absent = TRUE)
CasosNuevosPorComuna<-na.omit(CasosNuevosPorComuna)
CasosNuevosPorComuna<-CasosNuevosPorComuna[,c("fecha","Poblacion","codigo_comuna","casos_confirmado_comuna"),with=FALSE]


if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_CASOS_CONFIRMADO_COMUNA"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_CASOS_CONFIRMADO_COMUNA")
    hana_write_table(jdbcConnection = hanaConnection,df=CasosNuevosPorComuna,nombre = "COVID_CASOS_CONFIRMADO_COMUNA")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=CasosNuevosPorComuna,nombre = "COVID_CASOS_CONFIRMADO_COMUNA")
  }
  
}



# 2.1 Casos activos por comuna ----
CasosActualesPorComuna<-data.table::fread("./data/producto19_CasosActivosPorComuna_std.csv",sep=";",dec=",")
CasosActualesPorComuna[,name:=NULL]
CasosActualesPorComuna[,Fecha:=as.Date(Fecha)]
CasosActualesPorComuna[,semana:= as.Date(unique(cut(Fecha, "week"))),by=seq_len(nrow(CasosActualesPorComuna))]
data.table::setnames(CasosActualesPorComuna,old=c("Codigo comuna","Casos activos"),new=c("codigo_comuna","Casos_actuales"))
CasosActualesPorComuna<-CasosActualesPorComuna[,c("Fecha","semana","codigo_comuna","Casos_actuales"),with=FALSE]
CasosActualesPorComuna<-CasosActualesPorComuna[!is.na(codigo_comuna)]
CasosActualesPorComuna[,Fecha_min:=min(Fecha,na.rm = TRUE),by=c("semana","codigo_comuna")]
CasosActualesPorComuna[,Fecha_max:=max(Fecha,na.rm = TRUE),by=c("semana","codigo_comuna")]
CasosActualesPorComuna[,Casos_actuales_ini_cond:=Fecha==Fecha_min,by=seq_len(nrow(CasosActualesPorComuna))]
CasosActualesPorComuna[,Casos_actuales_max_cond:=Fecha==Fecha_max,by=seq_len(nrow(CasosActualesPorComuna))]

A=CasosActualesPorComuna[Casos_actuales_ini_cond==TRUE][,Casos_actuales_ini_cond:=NULL][,Casos_actuales_ini:=Casos_actuales][,Casos_actuales:=NULL]
A=A[,c("semana","codigo_comuna","Casos_actuales_ini"),with=FALSE]
B=CasosActualesPorComuna[Casos_actuales_max_cond==TRUE][,Casos_actuales_max_cond:=NULL][,Casos_actuales_fin:=Casos_actuales][,Casos_actuales:=NULL]
B=B[,c("semana","codigo_comuna","Casos_actuales_fin"),with=FALSE]

CasosActualesPorComuna<-CasosActualesPorComuna[,.(Casos_actuales_prom=mean(Casos_actuales,na.rm = TRUE)),by=c("semana","codigo_comuna")]
CasosActualesPorComuna<-A[CasosActualesPorComuna,on=c("semana","codigo_comuna")]
CasosActualesPorComuna<-B[CasosActualesPorComuna,on=c("semana","codigo_comuna")]

data.table::fwrite(CasosActualesPorComuna,file="caso_activos.csv",sep=";",dec=",")

if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_CASOS_ACTIVOS"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_CASOS_ACTIVOS")
    hana_write_table(jdbcConnection = hanaConnection,df=CasosActualesPorComuna,nombre = "COVID_CASOS_ACTIVOS")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=CasosActualesPorComuna,nombre = "COVID_CASOS_ACTIVOS")
  }
  
}

# 2.2 positvidad por region ----

listfiles<-list.files("./data",patter="producto4_",full.names = TRUE)
CasosActivosPorRegion<-base::lapply(listfiles,data.table::fread)
CasosActivosPorRegion <- data.table::rbindlist(CasosActivosPorRegion,fill=TRUE)
for( j in 1:nrow(emolRegion))
{
  CasosActivosPorRegion[Region==emolRegion$minCienciaRegiones[j],Region:=emolRegion$Region[j]]
}

CasosActivosPorRegion[,fecha:=paste0(head(unlist(base::strsplit(name,split="-")),3),collapse = "-"),by=seq_len(nrow(CasosActivosPorRegion))]
CasosActivosPorRegion[,name:=NULL]
data.table::setnames(CasosActivosPorRegion,c("Region","Casos nuevos totales","Casos nuevos"),c("region_residencia","casos_nuevos_totales","casos_nuevos"),skip_absent = TRUE)
CasosActivosPorRegion[is.na(casos_nuevos_totales),casos_nuevos_totales:=casos_nuevos]
CasosActivosPorRegion<-CasosActivosPorRegion[,c("fecha","region_residencia","casos_nuevos_totales"),with=FALSE]
CasosActivosPorRegion[,fecha:=as.Date(fecha)]
# 
pcrPorRegion<-data.table::fread("./data/producto7_PCR_std.csv")
for( j in 1:nrow(emolRegion))
{
  pcrPorRegion[Region==emolRegion$minCienciaRegiones[j],Region:=emolRegion$Region[j]]
}
pcrPorRegion[,name:=NULL]
data.table::setnames(pcrPorRegion,c("Region","Codigo region","Poblacion","fecha","numero"),c("region_residencia","codigo_region","Poblacion","fecha","numero_pcr"))
pcrPorRegion[,Poblacion:=NULL]
pcrPorRegion[,fecha:=as.Date(fecha)]


positividadRegional<-CasosActivosPorRegion[pcrPorRegion,on=c("fecha","region_residencia")]
positividadRegional<-positividadRegional[,c("fecha","codigo_region","region_residencia","casos_nuevos_totales","numero_pcr")]
#positividadRegional<-na.omit(positividadRegional)
positividadRegional<-funciones_fill_na(positividadRegional)
positividadRegional[,fecha:=as.Date(fecha)]
if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_CASOS_ACTIVOS_REGIONAL_PCR"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_CASOS_ACTIVOS_REGIONAL_PCR")
    hana_write_table(jdbcConnection = hanaConnection,df=positividadRegional,nombre = "COVID_CASOS_ACTIVOS_REGIONAL_PCR")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=positividadRegional,nombre = "COVID_CASOS_ACTIVOS_REGIONAL_PCR")
  }
  
}
# 2.2 Casos UCI regional y nacional----
camasUCI<-data.table::fread("./data/producto52_Camas_UCI_std.csv",sep=";",dec=",")
camasUCI[,Fecha:=as.character(Fecha)]
camasUCI[,Fecha:=raster::trim(Fecha),by=seq_len(nrow(camasUCI))]
camasUCI[,Fecha:=as.Date(Fecha)]
camasUCI[,name:=NULL]
UCI<-dcast(camasUCI, formula=Region+Fecha~Serie,value.var="Casos")
UCI[,Fecha:=as.Date(Fecha)]
for( j in 1:nrow(emolRegion))
{
  UCI[Region==emolRegion$minCienciaRegiones[j],Region:=emolRegion$Region[j]]
}
data.table::setnames(UCI,"Region","region_residencia")
uciNacional=UCI[region_residencia=="Total"]
UCI=UCI[region_residencia!="Total"]
marcoRegion<-unique(marcoTotal[,c("region_residencia","codigo_region")])
UCI=marcoRegion[UCI,on="region_residencia"]

old=c('Camas UCI habilitadas','Camas UCI ocupadas COVID-19','Camas UCI ocupadas no COVID-19','Camas base (2019)')
new=c('CAMAS_UCI_HABILITADAS','CAMAS_UCI_OCUPADAS_COVID_19','CAMAS_UCI_OCUPADAS_NO_COVID_19','CAMAS_BASE_2019')

data.table::setnames(UCI,old=old,new=new)
data.table::setnames(uciNacional,old=old,new=new)

if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_UCI_REGIONAL"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_UCI_REGIONAL")
    hana_write_table(jdbcConnection = hanaConnection,df=UCI,nombre = "COVID_UCI_REGIONAL")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=UCI,nombre = "COVID_UCI_REGIONAL")
  }
  
}

if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_UCI_NACIONAL"))
  {
   
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_UCI_NACIONAL")
    hana_write_table(jdbcConnection = hanaConnection,df=uciNacional,nombre = "COVID_UCI_NACIONAL")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=uciNacional,nombre = "COVID_UCI_NACIONAL")
  }
  
}

# 2.3 Positividad----

Positividad<-data.table::fread("./data/producto65_PositividadPorComuna_std.csv",sep=";",dec=",")
data.table::setnames(Positividad,old=c("Codigo region","Region","Codigo comuna","Comuna","Positividad")
                     ,new=c("codigo_region","region_residencia","codigo_comuna","comuna_residencia","positividad"))
Positividad[,name:=NULL]
FechaPositividad<-unique(Positividad[,c("Fecha"),with=FALSE])
FechaPositividad[,Fecha:=as.character(Fecha)]
FechaPositividad[,Fecha:=raster::trim(Fecha),by=seq_len(nrow(FechaPositividad))]
FechaPositividad[,Fecha:=as.Date(Fecha)]
FechaPositividad[,semana:= as.Date(unique(cut(Fecha, "week"))),by=seq_len(nrow(FechaPositividad))]
Positividad[,Fecha:=as.Date(Fecha)]
Positividad<-FechaPositividad[Positividad,on="Fecha"]
Positividad<-Positividad[,.(positividad=mean(positividad,na.rm=TRUE),Poblacion=unique(Poblacion)),by=c("codigo_comuna","semana")]
Positividad<-funciones_fill_na(Positividad)

if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_POSITIVIDAD_COMUNAL"))
  {
    
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_POSITIVIDAD_COMUNAL")
    hana_write_table(jdbcConnection = hanaConnection,df=Positividad,nombre = "COVID_POSITIVIDAD_COMUNAL")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=Positividad,nombre = "COVID_POSITIVIDAD_COMUNAL")
  }
  
}

poblacion<-unique(Positividad[,c("codigo_comuna","Poblacion")])

if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_POBLACION_COMUNAL"))
  {
    
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_POBLACION_COMUNAL")
    hana_write_table(jdbcConnection = hanaConnection,df=poblacion,nombre = "COVID_POBLACION_COMUNAL")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=poblacion,nombre = "COVID_POBLACION_COMUNAL")
  }
  
}



# 2.4 Numero reproductivo efectivo Re provincial Regional Nacional ----

re_prov<-data.table::fread("./data/producto54_r.provincial_n.csv",sep=";",dec=",")
re_prov[,fecha:=as.character(fecha)]
re_prov[,fecha:=raster::trim(fecha),by=seq_len(nrow(re_prov))]
re_prov[,fecha:=as.Date(fecha)]
re_prov[,name:=NULL]
re_reg<-data.table::fread("./data/producto54_r.regional_n.csv",sep=";",dec=",")
re_reg[,fecha:=as.Date(fecha)]
re_reg[,name:=NULL]
re_nac<-data.table::fread("./data/producto54_r.nacional_n.csv",sep=";",dec=",")
re_nac[,fecha:=as.Date(fecha)]
re_nac[,name:=NULL]



# 2.5 etapa paso a paso semanal  ----
pasoapasoD<-data.table::fread("./data/producto74_paso_a_paso_std.csv",sep=";",dec=",")
pasoapasoD[,Fecha:=as.Date(Fecha)]
if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_PASO_DIARIO"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_PASO_DIARIO")
    hana_write_table(jdbcConnection = hanaConnection,df=pasoapasoD,nombre = "COVID_PASO_DIARIO")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=pasoapasoD,nombre = "COVID_PASO_DIARIO")
  }
  
}
pasoapaso<-pasoapasoD[,.(Paso=min(Paso)),by=c("codigo_region","region_residencia","codigo_comuna","comuna_residencia","Fecha","name")]
fechaPasoapaso<-unique(pasoapaso[,c("Fecha")])
fechaPasoapaso[,Fecha:=as.character(Fecha)]
fechaPasoapaso[,Fecha:=raster::trim(Fecha),by=seq_len(nrow(fechaPasoapaso))]
fechaPasoapaso<-unique(fechaPasoapaso[,c("Fecha")])
fechaPasoapaso[,Fecha:=as.Date(Fecha)]
fechaPasoapaso[,semana:= as.Date(unique(cut(Fecha, "week"))),by=seq_len(nrow(fechaPasoapaso))]
pasoapaso[,Fecha:=as.character(Fecha)]
pasoapaso[,Fecha:=raster::trim(Fecha),by=seq_len(nrow(pasoapaso))]
pasoapaso[,Fecha:=as.Date(Fecha)]
pasoapaso<-fechaPasoapaso[pasoapaso,on="Fecha"]
pasoapaso[,max_fecha_sem:=max(Fecha),by=c("semana","codigo_comuna")]
pasoapaso=pasoapaso[Fecha==max_fecha_sem]
pasoapaso<-unique(pasoapaso[,c( "semana","codigo_comuna","Paso")])

if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_PASO"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_PASO")
    hana_write_table(jdbcConnection = hanaConnection,df=pasoapaso,nombre = "COVID_PASO")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=pasoapaso,nombre = "COVID_PASO")
  }
  
}
# 2.6 marco total comunas ----

numericas = c('codigo_region','codigo_provincia','codigo_comuna')
marcoTotal[ , c(numericas):=lapply(.SD, as.numeric), .SDcols = numericas]
if(FALSE)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_DATOS_COMUNAS"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_DATOS_COMUNAS")
    hana_write_table(jdbcConnection = hanaConnection,df=marcoTotal,nombre = "COVID_DATOS_COMUNAS")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=marcoTotal,nombre = "COVID_DATOS_COMUNAS")
  }
  
}

comuna_km2<-scraper_wiki_table()
comuna_km2<-funciones_fill_na(comuna_km2)
if(FALSE)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_COMUNAS_KM2"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_COMUNAS_KM2")
    hana_write_table(jdbcConnection = hanaConnection,df=comuna_km2,nombre = "COVID_COMUNAS_KM2")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=comuna_km2,nombre = "COVID_COMUNAS_KM2")
  }
  
}



# 2.5 etapa paso a paso semanal  ----
pasoapaso<-data.table::fread("./data/producto74_paso_a_paso_std.csv",sep=";",dec=",")
pasoapaso<-pasoapaso[,.(Paso=min(Paso)),by=c("codigo_region","region_residencia","codigo_comuna","comuna_residencia","Fecha")]
fechaPasoapaso<-unique(pasoapaso[,c("Fecha")])
fechaPasoapaso[,Fecha:=as.character(Fecha)]
fechaPasoapaso[,Fecha:=raster::trim(Fecha),by=seq_len(nrow(fechaPasoapaso))]
fechaPasoapaso<-unique(fechaPasoapaso[,c("Fecha")])
fechaPasoapaso[,Fecha:=as.Date(Fecha)]
fechaPasoapaso[,semana:= as.Date(unique(cut(Fecha, "week"))),by=seq_len(nrow(fechaPasoapaso))]
pasoapaso[,Fecha:=as.character(Fecha)]
pasoapaso[,Fecha:=raster::trim(Fecha),by=seq_len(nrow(pasoapaso))]
pasoapaso[,Fecha:=as.Date(Fecha)]
pasoapaso<-fechaPasoapaso[pasoapaso,on="Fecha"]
pasoapaso[,max_fecha_sem:=max(Fecha),by=c("semana","codigo_comuna")]
pasoapaso=pasoapaso[Fecha==max_fecha_sem]
pasoapaso<-unique(pasoapaso[,c( "semana","codigo_comuna","Paso")])
if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_PASO"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_PASO")
    hana_write_table(jdbcConnection = hanaConnection,df=pasoapaso,nombre = "COVID_PASO")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=pasoapaso,nombre = "COVID_PASO")
  }
  
}
# 2.9 movilidad comunas ----

numericas = c('codigo_region','codigo_comuna')
movilidad<-data.table::fread("./data/producto41_BIPComuna_std.csv",sep=";",dec=",")
movilidad[,c("Comuna","name"):=list(NULL,NULL)]
data.table::setnames(movilidad,old="Codigo comuna" ,new='codigo_comuna')
movilidad<-na.omit(movilidad)
movilidad<-unique(movilidad)
movilidad[,Fecha:=as.Date(Fecha)]
if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_COVID_MOVILIDAD_COMUNAS"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="COVID_MOVILIDAD_COMUNAS")
    hana_write_table(jdbcConnection = hanaConnection,df=movilidad,nombre = "COVID_MOVILIDAD_COMUNAS")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=movilidad,nombre = "COVID_MOVILIDAD_COMUNAS")
  }
  
}



# 2.9 movilidad comunas ----
TiendasGeolocalizaion<-data.table::setDT(openxlsx::read.xlsx(
  xlsxFile="./xlsx/dataTiendasCencoComuna.xlsx",
  sheet = "Locales"))
TiendasGeolocalizaion[,SAP:=as.character(SAP)]
TiendasGeolocalizaion[is.na(SAP),SAP:="-"]

if(actualizar)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_GEOLOCALIZACION_TIENDAS_COMUNAS"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="GEOLOCALIZACION_TIENDAS_COMUNAS")
    hana_write_table(jdbcConnection = hanaConnection,df=TiendasGeolocalizaion,nombre = "GEOLOCALIZACION_TIENDAS_COMUNAS")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=TiendasGeolocalizaion,nombre = "GEOLOCALIZACION_TIENDAS_COMUNAS")
  }
  
}

hana_public_table(jdbcConnection=hanaConnection,tablain="GEOLOCALIZACION_TIENDAS_COMUNAS")
hana_public_table(jdbcConnection=hanaConnection,tablain="COVID_PASO")


MARCO_COMUNAL<-hana_get_complete_table(jdbcConnection=hanaConnection,nombre="COVID_DATOS_COMUNAS")
POBLACION_COMUNAL<-hana_get_complete_table(jdbcConnection=hanaConnection,nombre="COVID_POBLACION_COMUNAL")
SUPERFICIE_COMUNAL<-hana_get_complete_table(jdbcConnection=hanaConnection,nombre="COVID_COMUNAS_KM2")
PASO_COMUNAL<-hana_get_complete_table(jdbcConnection=hanaConnection,nombre="COVID_PASO")
PASO_COMUNAL[,SEMANA:=as.Date(SEMANA)]
UCI_NACIONAL<-hana_get_complete_table_week(jdbcConnection =hanaConnection ,nombre="COVID_UCI_NACIONAL")
cols_1<-c("FECHA","SEMANA")
UCI_NACIONAL[,(cols_1) := lapply(.SD,as.Date), .SDcols=c("FECHA","SEMANA")]
data.table::fwrite(UCI_NACIONAL,file="./data_procesada/UCI_NACIONAL.csv",sep=";",dec=",")
UCI_REGIONAL<-hana_get_complete_table_week(jdbcConnection =hanaConnection ,nombre="COVID_UCI_REGIONAL")
UCI_REGIONAL[,(cols_1) := lapply(.SD,as.Date), .SDcols=c("FECHA","SEMANA")]
data.table::fwrite(UCI_REGIONAL,file="./data_procesada/UCI_REGIONAL.csv",sep=";",dec=",")
COVID_CASOS_ACTIVOS=hana_get_complete_table(jdbcConnection=hanaConnection,nombre="COVID_CASOS_ACTIVOS")
COVID_CASOS_ACTIVOS[,("SEMANA") := lapply(.SD,as.Date), .SDcols=c("SEMANA")]
data.table::fwrite(COVID_CASOS_ACTIVOS,file="./data_procesada/COVID_CASOS_ACTIVOS.csv",sep=";",dec=",")
COVID_CASOS_POSITIVIDAD=hana_get_complete_table_week(jdbcConnection=hanaConnection,nombre="COVID_CASOS_ACTIVOS_REGIONAL_PCR")
COVID_CASOS_POSITIVIDAD[,(cols_1) := lapply(.SD,as.Date), .SDcols=c("FECHA","SEMANA")]
data.table::fwrite(COVID_CASOS_POSITIVIDAD,file="./data_procesada/COVID_CASOS_POSITIVIDAD.csv",sep=";",dec=",")
PASO_ACTUAL=hana_get_complete_table(jdbcConnection=hanaConnection,nombre="GEOLOCALIZACION_FASE_ACTUAL_EMOL")[,c("CODIGO_COMUNA","PASO","PASO_PROB"),with=FALSE][,CODIGO_COMUNA:=as.character(CODIGO_COMUNA)]
data.table::fwrite(PASO_ACTUAL,file="./data_procesada/PASO_ACTUAL.csv",sep=";",dec=",")
# 1.1 Tablas externas 
COVID_CASOS_CONFIRMADOS_COMUNAS_QUERY=hana_get_complete_table_externa_week(jdbcConnection=hanaConnection,nombre="AA_FCAST_CASOS_NUEVOS")
data.table::fwrite(COVID_CASOS_CONFIRMADOS_COMUNAS_QUERY,file="./data_procesada/COVID_CASOS_CONFIRMADOS_COMUNAS_QUERY.csv",sep=";",dec=",")
COVID_CASOS_CONFIRMADOS_COMUNAS=POBLACION_COMUNAL[COVID_CASOS_CONFIRMADOS_COMUNAS_QUERY,on="CODIGO_COMUNA"]
COVID_CASOS_CONFIRMADOS_COMUNAS<-COVID_CASOS_CONFIRMADOS_COMUNAS[!is.na(CASOS_CONFIRMADO_COMUNA),c('FECHA','POBLACION','CODIGO_COMUNA','CASOS_CONFIRMADO_COMUNA','SEMANA'),with=FALSE]
COVID_CASOS_CONFIRMADOS_COMUNAS[,(cols_1) := lapply(.SD,as.Date), .SDcols=c("FECHA","SEMANA")]
if(borrar)
{
  do.call(file.remove,list(list.files("~/covid-19/data", full.names = TRUE)))
}



emol<-scraper_emol_table()
MARCO_COMUNAL<-hana_get_complete_table(jdbcConnection=hanaConnection,nombre="COVID_DATOS_COMUNAS")
emol[,COMUNA_RESIDENCIA:=unlist(strsplit(COMUNA_RESIDENCIA_DESC,"Desde"))[1],seq_len(nrow(emol))]
emol[,Paso_prob:=unlist(strsplit(COMUNA_RESIDENCIA_DESC,"Desde"))[2],seq_len(nrow(emol))]
emol[,COMUNA_RESIDENCIA:=gsub(">","",COMUNA_RESIDENCIA),by=seq_len(nrow(emol))]
emol[,COMUNA_RESIDENCIA:=raster::trim(COMUNA_RESIDENCIA),by=seq_len(nrow(emol))]
emol[,Paso_prob:=raster::trim(Paso_prob),by=seq_len(nrow(emol))]
emol[,Paso_prob:=tail(unlist(strsplit(Paso_prob," ")),1),seq_len(nrow(emol))]
emol[,Paso_prob:=raster::trim(Paso_prob),by=seq_len(nrow(emol))]

emolComuna<-openxlsx::read.xlsx("~/PROYECTO_ALLOCATION/xlsx/emolComuna.xlsx")
for(j in 1:nrow(emolComuna))
{ 
  emol[COMUNA_RESIDENCIA== emolComuna$emol_comuna[j],COMUNA_RESIDENCIA:=emolComuna$comuna[j]]
}
emol<-MARCO_COMUNAL[emol,on="COMUNA_RESIDENCIA"]
#
emolDaily<-data.table::copy(emol)
emolDaily[,FECHA:=Sys.Date()]
emolDaily<-emolDaily[,c("CODIGO_COMUNA","PASO","FECHA"),with=FALSE]
if(EMOL)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_GEOLOCALIZACION_FASE_ACTUAL_EMOL_DAILY"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="GEOLOCALIZACION_FASE_ACTUAL_EMOL_DAILY_AUX")
    hana_write_table(jdbcConnection = hanaConnection,df=emolDaily,nombre = "GEOLOCALIZACION_FASE_ACTUAL_EMOL_DAILY_AUX")
    hana_insert_table(jdbcConnection = hanaConnection,tablain="GEOLOCALIZACION_FASE_ACTUAL_EMOL_DAILY_AUX",tablafin="GEOLOCALIZACION_FASE_ACTUAL_EMOL_DAILY")
    hana_drop_table(jdbcConnection = hanaConnection,nombre="GEOLOCALIZACION_FASE_ACTUAL_EMOL_DAILY_AUX")
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=emolDaily,nombre = "GEOLOCALIZACION_FASE_ACTUAL_EMOL_DAILY")
  }
  
}



#

emolEtapas<-openxlsx::read.xlsx("~/PROYECTO_ALLOCATION/xlsx/emolEtapas.xlsx")
for(i in 1:nrow(emolEtapas))
{
  emol[Paso_prob==emolEtapas$ETAPAS_DESC[i],PASO:=emolEtapas$ETAPAS[i]]
}
#
emol<-emol[,c("CODIGO_COMUNA","COMUNA_RESIDENCIA","PASO","Paso_prob"),with=FALSE]
emol[,Paso_prob:=!is.na(Paso_prob)]
if(EMOL)
{
  if(DBI::dbExistsTable(hanaConnection, "FC_GEOLOCALIZACION_FASE_ACTUAL_EMOL"))
  {
    hana_drop_table(jdbcConnection = hanaConnection,nombre="GEOLOCALIZACION_FASE_ACTUAL_EMOL")
    hana_write_table(jdbcConnection = hanaConnection,df=emol,nombre = "GEOLOCALIZACION_FASE_ACTUAL_EMOL")
    
  }else
  {
    hana_write_table(jdbcConnection = hanaConnection,df=emol,nombre = "GEOLOCALIZACION_FASE_ACTUAL_EMOL")
  }
  
}
end_time=Sys.time()
print(end_time-start_time)

