#0. SET WORKING DIRECTORY,CLEAN SCREEN, REMOVE VARIABLES----
setwd('~/covid-19')
cat("\014") 
graphics.off()
rm(list = ls())
options(java.parameters = "-Xmx8g")
options(encoding = 'UTF-8')
library(dplyr)
library(data.table)
library(ggplot2)
library(scales)
library(forecast)
library(reshape)
library(e1071)
library(EpiEstim)
library(parallel)
source("helper_scraper.R")
source("helper_hana.R")
source("funciones.R")
source('~/connections/connections.R')
start_time=Sys.time()
print(start_time)
hanaConnection<-hana_connect()
# 1. CARGAR DATA HANA ----
if(TRUE)
{
  MARCO_COMUNAL<-hana_get_complete_table(jdbcConnection=hanaConnection,nombre="COVID_DATOS_COMUNAS")
  POBLACION_COMUNAL<-hana_get_complete_table(jdbcConnection=hanaConnection,nombre="COVID_POBLACION_COMUNAL")
  SUPERFICIE_COMUNAL<-hana_get_complete_table(jdbcConnection=hanaConnection,nombre="COVID_COMUNAS_KM2")
  PASO_COMUNAL<-hana_get_complete_table(jdbcConnection=hanaConnection,nombre="COVID_PASO")
  PASO_COMUNAL[,SEMANA:=as.Date(SEMANA)]
  UCI_NACIONAL<-hana_get_complete_table_week(jdbcConnection =hanaConnection ,nombre="COVID_UCI_NACIONAL")
  cols_1<-c("FECHA","SEMANA")
  UCI_NACIONAL[,(cols_1) := lapply(.SD,as.Date), .SDcols=c("FECHA","SEMANA")]
  UCI_REGIONAL<-hana_get_complete_table_week(jdbcConnection =hanaConnection ,nombre="COVID_UCI_REGIONAL")
  UCI_REGIONAL[,(cols_1) := lapply(.SD,as.Date), .SDcols=c("FECHA","SEMANA")]
  COVID_CASOS_ACTIVOS=hana_get_complete_table(jdbcConnection=hanaConnection,nombre="COVID_CASOS_ACTIVOS")
  COVID_CASOS_ACTIVOS[,("SEMANA") := lapply(.SD,as.Date), .SDcols=c("SEMANA")]
  COVID_CASOS_POSITIVIDAD=hana_get_complete_table_week(jdbcConnection=hanaConnection,nombre="COVID_CASOS_ACTIVOS_REGIONAL_PCR")
  COVID_CASOS_POSITIVIDAD[,(cols_1) := lapply(.SD,as.Date), .SDcols=c("FECHA","SEMANA")]
  # 1.1 Tablas externas 
  COVID_CASOS_CONFIRMADOS_COMUNAS_QUERY=hana_get_complete_table_externa_week(jdbcConnection=hanaConnection,nombre="AA_FCAST_CASOS_NUEVOS")
  COVID_CASOS_CONFIRMADOS_COMUNAS=POBLACION_COMUNAL[COVID_CASOS_CONFIRMADOS_COMUNAS_QUERY,on="CODIGO_COMUNA"]
  COVID_CASOS_CONFIRMADOS_COMUNAS<-COVID_CASOS_CONFIRMADOS_COMUNAS[!is.na(CASOS_CONFIRMADO_COMUNA),c('FECHA','POBLACION','CODIGO_COMUNA','CASOS_CONFIRMADO_COMUNA','SEMANA'),with=FALSE]
  COVID_CASOS_CONFIRMADOS_COMUNAS[,(cols_1) := lapply(.SD,as.Date), .SDcols=c("FECHA","SEMANA")]
}else{
  
  MARCO_COMUNAL<-data.table::setDT(openxlsx::read.xlsx("./data/MARCO_COMUNAL.xlsx"))
  POBLACION_COMUNAL<-data.table::setDT(openxlsx::read.xlsx("./data/POBLACION_COMUNAL.xlsx"))
  SUPERFICIE_COMUNAL<-data.table::setDT(openxlsx::read.xlsx("./data/SUPERFICIE_COMUNAL.xlsx"))
  
  COVID_CASOS_POSITIVIDAD<-data.table::fread("./data/COVID_CASOS_POSITIVIDAD.csv",sep=";",dec=",",encoding ="Latin-1")
  COVID_CASOS_CONFIRMADOS_COMUNAS<-data.table::fread("./data/COVID_CASOS_CONFIRMADOS_COMUNAS.csv",sep=";",dec=",",encoding ="Latin-1",colClasses = c('Date','numeric','numeric','numeric','Date'))
}

no_cores <- parallel::detectCores()-1
clust <- makeCluster(no_cores) #This line will take time
clusterExport(clust, varlist = ls(pattern ="funciones_" ), envir = .GlobalEnv)
clusterEvalQ(clust, {
  library(RJDBC)
  library(dplyr)
  library(data.table)
  library(ggplot2)
  library(scales)
  library(forecast)
  library(reshape)
  library(e1071)
  library(EpiEstim)
  library(caret)
 
})

# 2. GENERAR DATA CRITERIOS COVID

# Definimos el umbral de la prediccion

nSemanas<-4# numero de semanas
primerDia<-as.Date(cut(as.Date(Sys.Date()), "week"))
SEMANAS_COM<-seq(primerDia,primerDia+7*(nSemanas),by="days")
SEMANAS_COM<-as.Date(unique(cut(as.Date(SEMANAS_COM), "week")))
SEMANAS=SEMANAS_COM[1:nSemanas]
FECHAS=seq(primerDia,tail(SEMANAS_COM,1)-1,by="days")
nDias<-length(FECHAS)

data.table::setnames(COVID_CASOS_CONFIRMADOS_COMUNAS,"CASOS_CONFIRMADO_COMUNA","CASOS")
COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT<-split(COVID_CASOS_CONFIRMADOS_COMUNAS,by="CODIGO_COMUNA")
COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT=parallel::parLapply(cl=clust,X=COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT,fun=funciones_ordenar_fecha_semana,tipo="FECHA")
COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT=parallel::parLapply(cl=clust,X=COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT,fun=funciones_fill_gap)

COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS<-parallel::parLapply(cl=clust,X=COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT,fun=funciones_create_ts,var="CASOS",frecuencia="diaria")
if(FALSE){
  COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS_MOD<-parallel::parLapply(cl=clust,X=COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS,fun=funciones_modelos_var)
  COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS_MOD_PREDICT<-parallel::parLapply(cl=clust,X=COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS_MOD,fun=funciones_prediccion_var_mod,n.ahead=nDias,CI=0.95)
  
}else{
  COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS_MOD_PREDICT=COVID_CASOS_CONFIRMADOS_COMUNAS_QUERY[is.na(CASOS_CONFIRMADO_COMUNA)]
  cols_1=c("FECHA","SEMANA")
  COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS_MOD_PREDICT[,(cols_1) := lapply(.SD,as.Date), .SDcols=cols_1] 
  COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS_MOD_PREDICT[,c('fit','lower','upper','METODO','ME','RMSE','MAE','MPE','MAPE','MASE','ACF1'):=list(YHAT,YHAT_LOWER,YHAT_UPPER,"ALI",0,0,0,0,0,0,0)]
  COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS_MOD_PREDICT<-COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS_MOD_PREDICT[,c('FECHA','CODIGO_COMUNA','fit','lower','upper','METODO','ME','RMSE','MAE','MPE','MAPE','MASE','ACF1'),with=FALSE]
  COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS_MOD_PREDICT<-split(COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS_MOD_PREDICT,by="CODIGO_COMUNA")
  COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT=parallel::parLapply(cl=clust,X=COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT,fun=funciones_ordenar_fecha_semana,tipo="FECHA")
  
}


COVID_CASOS_CONFIRMADOS_REGION<-COVID_CASOS_POSITIVIDAD[,c("FECHA","SEMANA","CODIGO_REGION","CASOS_NUEVOS_TOTALES"),with=FALSE]
data.table::setnames(COVID_CASOS_CONFIRMADOS_REGION,"CASOS_NUEVOS_TOTALES","CASOS_CONFIRMADO_REGION",skip_absent = TRUE)
POBLACION_REGION<-MARCO_COMUNAL[POBLACION_COMUNAL,on="CODIGO_COMUNA"]
POBLACION_REGION<-POBLACION_REGION[,.(POBLACION=sum(POBLACION,na.rm = TRUE)),by=c("CODIGO_REGION")]
COVID_CASOS_CONFIRMADOS_REGION=POBLACION_REGION[COVID_CASOS_CONFIRMADOS_REGION,on="CODIGO_REGION"]
COVID_CASOS_CONFIRMADOS_REGION_SPLIT<-split(COVID_CASOS_CONFIRMADOS_REGION,by="CODIGO_REGION")

COVID_CASOS_CONFIRMADOS_REGION_SPLIT=parallel::parLapply(cl=clust,X=COVID_CASOS_CONFIRMADOS_REGION_SPLIT,fun=funciones_ordenar_fecha_semana,tipo="FECHA")
COVID_CASOS_CONFIRMADOS_REGION_SPLIT_TS<-parallel::parLapply(cl=clust,X=COVID_CASOS_CONFIRMADOS_REGION_SPLIT,fun=funciones_create_ts,var="CASOS_CONFIRMADO_REGION",frecuencia="diaria")
COVID_CASOS_CONFIRMADOS_REGION_SPLIT_TS_MOD<-parallel::parLapply(cl=clust,X=COVID_CASOS_CONFIRMADOS_REGION_SPLIT_TS,fun=funciones_modelos_var)
COVID_CASOS_CONFIRMADOS_REGION_SPLIT_TS_MOD_PREDICT<-parallel::parLapply(cl=clust,X=COVID_CASOS_CONFIRMADOS_REGION_SPLIT_TS_MOD,fun=funciones_prediccion_var_mod,n.ahead=nDias,CI=0.95)



COVID_CASOS_CASOS_NUEVOS_REGIONAL<-COVID_CASOS_POSITIVIDAD
COVID_CASOS_CASOS_NUEVOS_REGIONAL_SPLIT<-split(COVID_CASOS_CASOS_NUEVOS_REGIONAL,by="REGION_RESIDENCIA")
COVID_CASOS_CASOS_NUEVOS_REGIONAL_SPLIT=parallel::parLapply(cl=clust,X=COVID_CASOS_CASOS_NUEVOS_REGIONAL_SPLIT,fun=funciones_ordenar_fecha_semana,tipo="FECHA")
COVID_CASOS_CASOS_NUEVOS_REGIONAL_SPLIT_TS<-parallel::parLapply(cl=clust,X=COVID_CASOS_CASOS_NUEVOS_REGIONAL_SPLIT,fun=funciones_create_ts,var="CASOS_NUEVOS_TOTALES",frecuencia="diaria")
COVID_PCR_REGIONAL_SPLIT_TS<-parallel::parLapply(cl=clust,X=COVID_CASOS_CASOS_NUEVOS_REGIONAL_SPLIT,fun=funciones_create_ts,var="NUMERO_PCR",frecuencia="diaria")

COVID_CASOS_CASOS_NUEVOS_REGIONAL_SPLIT_TS_MOD<-parallel::parLapply(cl=clust,X=COVID_CASOS_CASOS_NUEVOS_REGIONAL_SPLIT_TS,fun=funciones_modelos_var)
COVID_PCR_REGIONAL_SPLIT_TS_MOD<-parallel::parLapply(cl=clust,X=COVID_PCR_REGIONAL_SPLIT_TS,fun=funciones_modelos_var)

PCR_REGIONAL_TS_MOD_PREDICT<-parallel::parLapply(cl=clust,X=COVID_PCR_REGIONAL_SPLIT_TS_MOD,fun=funciones_prediccion_var_mod,n.ahead=nDias,CI=0.95)
CASOS_NUEVOS_REGIONAL_TS_MOD_PREDICT<-parallel::parLapply(cl=clust,X=COVID_CASOS_CASOS_NUEVOS_REGIONAL_SPLIT_TS_MOD,fun=funciones_prediccion_var_mod,n.ahead=nDias,CI=0.95)

UCI_NACIONAL_COVID<-funciones_ordenar_fecha_semana(UCI_NACIONAL,tipo = "DIARIA")
UCI_NACIONAL_COVID_TS<-funciones_create_ts(UCI_NACIONAL,var="CAMAS_UCI_OCUPADAS_COVID_19",frecuencia="diaria")
UCI_NACIONAL_COVID_TS_MOD<-funciones_modelos_var(UCI_NACIONAL_COVID_TS)
UCI_NACIONAL_COVID_TS_PRED_PREDICT<-funciones_prediccion_var_mod(UCI_NACIONAL_COVID_TS_MOD,n.ahead=nDias,CI=0.95)

UCI_NACIONAL_NO_COVID<-funciones_ordenar_fecha_semana(UCI_NACIONAL,tipo = "DIARIA")
UCI_NACIONAL_NO_COVID_TS<-funciones_create_ts(UCI_NACIONAL,var="CAMAS_UCI_OCUPADAS_NO_COVID_19",frecuencia="diaria")
UCI_NACIONAL_NO_COVID_TS_MOD<-funciones_modelos_var(UCI_NACIONAL_NO_COVID_TS)
UCI_NACIONAL_NO_COVID_TS_PRED_PREDICT<-funciones_prediccion_var_mod(UCI_NACIONAL_NO_COVID_TS_MOD,n.ahead=nDias,CI=0.95)

UCI_REGIONAL_COVID<-UCI_REGIONAL
UCI_REGIONAL_COVID_SPLIT<-split(UCI_REGIONAL_COVID,by="REGION_RESIDENCIA")
UCI_REGIONAL_COVID_SPLIT=parallel::parLapply(cl=clust,X=UCI_REGIONAL_COVID_SPLIT,fun=funciones_ordenar_fecha_semana,tipo="FECHA")
UCI_REGIONAL_COVID_SPLIT_TS<-parallel::parLapply(cl=clust,X=UCI_REGIONAL_COVID_SPLIT,fun=funciones_create_ts,var="CAMAS_UCI_OCUPADAS_COVID_19",frecuencia="diaria")
UCI_REGIONAL_COVID_SPLIT_TS_MOD<-parallel::parLapply(cl=clust,X=UCI_REGIONAL_COVID_SPLIT_TS,fun=funciones_modelos_var)
UCI_REGIONAL_COVID_SPLIT_TS_MOD_PREDICT<-parallel::parLapply(cl=clust,X=UCI_REGIONAL_COVID_SPLIT_TS_MOD,fun=funciones_prediccion_var_mod,n.ahead=nDias,CI=0.95)

UCI_REGIONAL_NO_COVID<-UCI_REGIONAL
UCI_REGIONAL_NO_COVID_SPLIT<-split(UCI_REGIONAL_COVID,by="REGION_RESIDENCIA")
UCI_REGIONAL_NO_COVID_SPLIT=parallel::parLapply(cl=clust,X=UCI_REGIONAL_NO_COVID_SPLIT,fun=funciones_ordenar_fecha_semana,tipo="FECHA")
UCI_REGIONAL_NO_COVID_SPLIT_TS<-parallel::parLapply(cl=clust,X=UCI_REGIONAL_NO_COVID_SPLIT,fun=funciones_create_ts,var="CAMAS_UCI_OCUPADAS_NO_COVID_19",frecuencia="diaria")
UCI_REGIONAL_NO_COVID_SPLIT_TS_MOD<-parallel::parLapply(cl=clust,X=UCI_REGIONAL_NO_COVID_SPLIT_TS,fun=funciones_modelos_var)
UCI_REGIONAL_NO_COVID_SPLIT_TS_MOD_PREDICT<-parallel::parLapply(cl=clust,X=UCI_REGIONAL_NO_COVID_SPLIT_TS_MOD,fun=funciones_prediccion_var_mod,n.ahead=nDias,CI=0.95)

COVID_CASOS_ACTIVOS=POBLACION_COMUNAL[COVID_CASOS_ACTIVOS,on="CODIGO_COMUNA"]
COVID_CASOS_ACTIVOS_SPLIT=split(COVID_CASOS_ACTIVOS,by="CODIGO_COMUNA")
COVID_CASOS_ACTIVOS_SPLIT=parallel::parLapply(cl=clust,X=COVID_CASOS_ACTIVOS_SPLIT,fun=funciones_ordenar_fecha_semana)
COVID_CASOS_ACTIVOS_SPLIT_TS<-parallel::parLapply(cl=clust,X=COVID_CASOS_ACTIVOS_SPLIT,fun=funciones_create_ts,var="CASOS_ACTUALES_FIN",frecuencia="semanal")
COVID_CASOS_ACTIVOS_SPLIT_TS_PREDICT<-parallel::parLapply(cl=clust,X=COVID_CASOS_ACTIVOS_SPLIT_TS,fun=funciones_prediccion_var,n.ahead=nSemanas,CI=0.95)
message("Predicciones ready")
#CALCULAMOS LAS PROBABILIDADES
prob_dat<-parallel::parLapply(cl=clust,COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT,funciones_umbral_velocidad_train)
message("Velocidad ready")
prob_dat<-data.table::rbindlist(prob_dat)
prob_dat=prob_dat[, .SD[c(.N)], by=c("SEMANA","CODIGO_COMUNA")][,c("SEMANA","CODIGO_COMUNA","R_UMBRAL"),with=FALSE]
prob_dat[,SEMANA:=as.Date(SEMANA)]
PASO_COMUNAL[,SEMANA:=as.Date(SEMANA)]
prob_dat<-PASO_COMUNAL[prob_dat,on=c("CODIGO_COMUNA","SEMANA")]
prob_dat<-split(prob_dat,by="CODIGO_COMUNA")
prob_dat<-parallel::parLapply(cl=clust,prob_dat,fun=funciones_add_var_dep)

prob_dat_regiones<-parallel::parLapply(cl=clust,COVID_CASOS_CONFIRMADOS_REGION_SPLIT,fun=funciones_umbral_casos_region)
prob_dat_regiones<-data.table::rbindlist(prob_dat_regiones)
prob_dat_regiones[,FECHA:=as.Date(FECHA)]
#prob_dat_regiones[, SEMANA:= as.Date(unique(cut(as.Date(FECHA), "week"))),by=seq_len(nrow(prob_dat_regiones))]
prob_dat_regiones=prob_dat_regiones[, .SD[c(.N)], by=c("SEMANA","CODIGO_REGION")]
prob_dat_regiones[,SEMANA:=as.Date(SEMANA)]
PASO_COMUNAL[,SEMANA:=as.Date(SEMANA)]


umbral_uci_nacional<-c(85,85,80,80)
prob_dat_uci_nacional<-funciones_umbral_casos_uci(dat=UCI_NACIONAL_COVID,umbral = umbral_uci_nacional)
#prob_dat_uci_nacional[, SEMANA:= as.Date(unique(cut(as.Date(FECHA), "week"))),by=seq_len(nrow(prob_dat_uci_nacional))]
prob_dat_uci_nacional=prob_dat_uci_nacional[, .SD[c(.N)], by="SEMANA"][,c("SEMANA","FASE_1","FASE_2","FASE_3","FASE_4"),with=FALSE]
#

umbral_uci_regional<-c(90,85,85,80)

prob_dat_uci_regional<-parallel::parLapply(cl=clust,UCI_REGIONAL_COVID_SPLIT,fun=funciones_umbral_casos_uci,umbral=umbral_uci_regional)
prob_dat_uci_regional<-data.table::rbindlist(prob_dat_uci_regional)
#prob_dat_uci_regional[, SEMANA:= as.Date(unique(cut(as.Date(FECHA), "week"))),by=seq_len(nrow(prob_dat_uci_regional))]
prob_dat_uci_regional=prob_dat_uci_regional[, .SD[c(.N)], by=c("SEMANA","REGION_RESIDENCIA")][,c("SEMANA","REGION_RESIDENCIA","FASE_1","FASE_2","FASE_3","FASE_4"),with=FALSE]




prob_positividad_regional<-parallel::parLapply(cl=clust,COVID_CASOS_CASOS_NUEVOS_REGIONAL_SPLIT,fun=funciones_umbral_positividad)
prob_positividad_regional<-data.table::rbindlist(prob_positividad_regional)
#prob_positividad_regional[, SEMANA:= as.Date(unique(cut(as.Date(FECHA), "week"))),by=seq_len(nrow(prob_positividad_regional))]
prob_positividad_regional=prob_positividad_regional[, .SD[c(.N)], by=c("SEMANA","REGION_RESIDENCIA")][,c("SEMANA","REGION_RESIDENCIA","FASE_POSITIVIDAD_1","FASE_POSITIVIDAD_2","FASE_POSITIVIDAD_3","FASE_POSITIVIDAD_4"),with=FALSE]





regionComuna<-unique(MARCO_COMUNAL[,c("CODIGO_COMUNA","CODIGO_REGION","REGION_RESIDENCIA")])

prob_positividad_regional<-regionComuna[prob_positividad_regional,on="REGION_RESIDENCIA",allow.cartesian=TRUE][,c("REGION_RESIDENCIA","CODIGO_REGION"):=list(NULL,NULL)]
prob_dat_uci_regional<-regionComuna[prob_dat_uci_regional,on="REGION_RESIDENCIA",allow.cartesian=TRUE][,c("REGION_RESIDENCIA","CODIGO_REGION"):=list(NULL,NULL)]
data.table::setnames(prob_dat_uci_regional,old=c("FASE_1","FASE_2","FASE_3","FASE_4"),new=c("FASE_REG_1","FASE_REG_2","FASE_REG_3","FASE_REG_4"),skip_absent = TRUE)

prob_dat_regiones<-regionComuna[prob_dat_regiones,on="CODIGO_REGION",allow.cartesian=TRUE]
prob_dat_regiones<-prob_dat_regiones[,c('SEMANA','CODIGO_COMUNA','DIMINUCION_SOTENIDA_2SEM','DIMINUCION_SOTENIDA_3SEM','TASA_PROYECTADA_FIT_50','TASA_PROYECTADA_FIT_25')]


prob_dat<-data.table::rbindlist(prob_dat)
#prob_dat[,c('DIMINUCION_SOTENIDA_2SEM','DIMINUCION_SOTENIDA_3SEM','TASA_PROYECTADA_FIT_50','TASA_PROYECTADA_FIT_25'):=list(NULL)]
prob_dat<-regionComuna[prob_dat,on="CODIGO_COMUNA"]
prob_dat<-prob_dat_uci_nacional[prob_dat,on="SEMANA"]
prob_dat<-prob_dat_uci_regional[prob_dat,on=c("SEMANA","CODIGO_COMUNA")]
prob_dat<-prob_positividad_regional[prob_dat,on=c("SEMANA","CODIGO_COMUNA")]
prob_dat<-prob_dat_regiones[prob_dat,on=c("SEMANA","CODIGO_COMUNA")]

prob_dat<-na.omit(prob_dat)


if(FALSE)
{
  dataCluster<-POBLACION_COMUNAL[SUPERFICIE_COMUNAL,on="CODIGO_COMUNA"]
  dataCluster[,DENSIDAD:=round(POBLACION/SUPERFICIE_KM_2,2)]
  dataCluster[,DENSIDAD:=scale(DENSIDAD)]
  dataCluster[,IDH_NUMERO:=scale(IDH_NUMERO)]
  cluster=MARCO_COMUNAL[dataCluster,on="CODIGO_COMUNA"]
  
  #nb<-NbClust::NbClust(cluster[,c("IDH_NUMERO","DENSIDAD")],method="kmeans")
  
  
  cluster[,CLUSTER:=1]
  cluster<-cluster[,c("CODIGO_COMUNA","CLUSTER"),with=FALSE]
}



if(FALSE)
{
  dataCluster<-POBLACION_COMUNAL[SUPERFICIE_COMUNAL,on="CODIGO_COMUNA"]
  dataCluster[,DENSIDAD:=round(POBLACION/SUPERFICIE_KM_2,2)]
  dataCluster[,DENSIDAD:=scale(DENSIDAD)]
  dataCluster[,IDH_NUMERO:=scale(IDH_NUMERO)]
  cluster=MARCO_COMUNAL[dataCluster,on="CODIGO_COMUNA"]
  
  #nb<-NbClust::NbClust(cluster[,c("IDH_NUMERO","DENSIDAD")],method="kmeans")
  
  
  cluster[,CLUSTER:=kmeans(cbind(IDH_NUMERO,DENSIDAD),centers=4)$cluster]
  cluster<-cluster[,c("CODIGO_COMUNA","CLUSTER"),with=FALSE]
}

if(TRUE)
{
  cluster=MARCO_COMUNAL
  cluster[,CLUSTER:=CODIGO_REGION]
  cluster<-cluster[,c("CODIGO_COMUNA","CLUSTER"),with=FALSE]
}

if(FALSE)
{
  cluster=MARCO_COMUNAL
  cluster[,CLUSTER:=CODIGO_REGION==13]
  cluster<-cluster[,c("CODIGO_COMUNA","CLUSTER"),with=FALSE]
}

if(FALSE)
{
  cluster=MARCO_COMUNAL
  cluster[,CLUSTER:="TODAS"]
  cluster<-cluster[,c("CODIGO_COMUNA","CLUSTER"),with=FALSE]
}

if(FALSE)
{
  cluster=MARCO_COMUNAL
  cluster[,CLUSTER:="CODIGO_COMUNA"]
  cluster<-unique(cluster[,c("CODIGO_COMUNA","CLUSTER"),with=FALSE])
}

prob_dat=cluster[prob_dat,on="CODIGO_COMUNA"]

prob_dat[, CAMBIOS:=sum(Y!=0),by=c("SEMANA")]

prob_dat=prob_dat[CAMBIOS!=0]


prob_dat_list<-split(prob_dat,by="CODIGO_COMUNA")

prob_dat_list<-parallel::parLapply(cl=clust,prob_dat_list,fun=funciones_sample)
prob_dat<-data.table::rbindlist(prob_dat_list)

trainData<-prob_dat[,c('Y','CLUSTER',"PASO","R_UMBRAL",'DIMINUCION_SOTENIDA_2SEM','DIMINUCION_SOTENIDA_3SEM','TASA_PROYECTADA_FIT_50','TASA_PROYECTADA_FIT_25',"FASE_1","FASE_2","FASE_3","FASE_4","FASE_REG_1","FASE_REG_2","FASE_REG_3","FASE_REG_4",
                       "FASE_POSITIVIDAD_1","FASE_POSITIVIDAD_2","FASE_POSITIVIDAD_3","FASE_POSITIVIDAD_4")]
trainData$Y<-factor(as.character(trainData$Y),levels=c("-1","0","1"),c("bajar","mantenerse","subir"))

fkt=c("PASO")
ckt=c("R_UMBRAL",'DIMINUCION_SOTENIDA_2SEM','DIMINUCION_SOTENIDA_3SEM','TASA_PROYECTADA_FIT_50','TASA_PROYECTADA_FIT_25',"FASE_1","FASE_2","FASE_3","FASE_4","FASE_REG_1","FASE_REG_2","FASE_REG_3","FASE_REG_4",
      "FASE_POSITIVIDAD_1","FASE_POSITIVIDAD_2","FASE_POSITIVIDAD_3","FASE_POSITIVIDAD_4")
trainData[ , (fkt) := lapply(.SD, factor,levels=c("1","2","3","4")), .SDcols = fkt]
trainData[ , (ckt) := lapply(.SD, factor,levels=c("TRUE","FALSE")), .SDcols = ckt]
trainData<-split(trainData,by="CLUSTER")
#mod<-parallel::parLapply(cl=clust,trainData,fun=funciones_naive_bayes)
mod<-parallel::parLapply(cl=clust,trainData,fun=funciones_naive_bayes_dev)

message("Entrenamiento de los modelos ready")
testData<-list()
pred<-list()
testDataUCI<-funciones_umbral_casos_uci_pred(dat=UCI_NACIONAL_COVID,pred_used_covid = UCI_NACIONAL_COVID_TS_PRED_PREDICT,pred_used_no_covid = UCI_NACIONAL_NO_COVID_TS_PRED_PREDICT,umbral=umbral_uci_nacional)
testDataUCI=Map(funciones_add_column, testDataUCI , names(testDataUCI))
testDataUCI=data.table::rbindlist(testDataUCI)
data.table::setnames(testDataUCI,old="name",new="ESCENARIOS")

testDataUCI<-testDataUCI[,c("SEMANA","ESCENARIOS","FASE_1","FASE_2","FASE_3","FASE_4")]
testDataUCI[,SEMANA:=as.Date(SEMANA)]


testDataUCI_REGIONAL<-list()
testPositividad_REGIONAL<-list()
for(j in unique(names(UCI_REGIONAL_COVID_SPLIT)))
{
  testPositividad_REGIONAL[[j]]<-list(dat=COVID_CASOS_CASOS_NUEVOS_REGIONAL_SPLIT[[j]],pred_pcr=PCR_REGIONAL_TS_MOD_PREDICT[[j]],pred_casos = CASOS_NUEVOS_REGIONAL_TS_MOD_PREDICT[[j]])
  testDataUCI_REGIONAL[[j]]<-list(dat=UCI_REGIONAL_COVID_SPLIT[[j]],pred_used_covid = UCI_REGIONAL_COVID_SPLIT_TS_MOD_PREDICT[[j]],pred_used_no_covid = UCI_REGIONAL_NO_COVID_SPLIT_TS_MOD_PREDICT[[j]])
}

testDataUCI_REGIONAL<-parallel::parLapply(cl=clust,X=testDataUCI_REGIONAL,fun=funciones_umbral_casos_uci_pred_parallel,regionComuna=regionComuna,umbral=umbral_uci_regional)
testPositividad_REGIONAL<-parallel::parLapply(cl=clust,X=testPositividad_REGIONAL,fun=funciones_umbral_positividad_pred_parallel,regionComuna=regionComuna)



testPositividad_REGIONAL<-data.table::rbindlist(testPositividad_REGIONAL)
#testPositividad_REGIONAL[, SEMANA:= as.Date(unique(cut(as.Date(FECHA), "week"))),by=seq_len(nrow(testPositividad_REGIONAL))]
testPositividad_REGIONAL<-split(testPositividad_REGIONAL,by=c("CODIGO_COMUNA","ESCENARIOS"))
testPositividad_REGIONAL<-parallel::parLapply(cl=clust,testPositividad_REGIONAL,fun = funciones_ordenar_fecha_semana,tipo="FECHA")
testPositividad_REGIONAL<-data.table::rbindlist(testPositividad_REGIONAL)
testPositividad_REGIONAL=testPositividad_REGIONAL[, .SD[c(.N)], by=c("SEMANA","CODIGO_COMUNA","ESCENARIOS")][,c("SEMANA","CODIGO_COMUNA","ESCENARIOS","FASE_POSITIVIDAD_1","FASE_POSITIVIDAD_2","FASE_POSITIVIDAD_3","FASE_POSITIVIDAD_4"),with=FALSE]



testDataUCI_REGIONAL<-data.table::rbindlist(testDataUCI_REGIONAL)
#testDataUCI_REGIONAL[, SEMANA:= as.Date(unique(cut(as.Date(FECHA), "week"))),by=seq_len(nrow(testDataUCI_REGIONAL))]
testDataUCI_REGIONAL<-split(testDataUCI_REGIONAL,by=c("CODIGO_COMUNA","ESCENARIOS"))
testDataUCI_REGIONAL<-parallel::parLapply(cl=clust,testDataUCI_REGIONAL,fun = funciones_ordenar_fecha_semana,tipo="FECHA")
testDataUCI_REGIONAL<-data.table::rbindlist(testDataUCI_REGIONAL)
testDataUCI_REGIONAL=testDataUCI_REGIONAL[, .SD[c(.N)], by=c("SEMANA","CODIGO_COMUNA","ESCENARIOS")][,c("SEMANA","CODIGO_COMUNA","ESCENARIOS","FASE_REG_1","FASE_REG_2","FASE_REG_3","FASE_REG_4"),with=FALSE]



testCASOS_CONFIRMADO_REGIONAL<-list()
for(w in unique(names(COVID_CASOS_CONFIRMADOS_REGION_SPLIT)))
{
  X<-list(dat=COVID_CASOS_CONFIRMADOS_REGION_SPLIT[[w]],pred_used = COVID_CASOS_CONFIRMADOS_REGION_SPLIT_TS_MOD_PREDICT[[w]])
  testCASOS_CONFIRMADO_REGIONAL[[w]]<-X
}
testCASOS_CONFIRMADO_REGIONAL<-parallel::parLapply(cl=clust,X=testCASOS_CONFIRMADO_REGIONAL,fun=funciones_umbral_casos_pred_regiones_parallel,regionComuna=regionComuna)
testCASOS_CONFIRMADO_REGIONAL<-data.table::rbindlist(testCASOS_CONFIRMADO_REGIONAL)
testCASOS_CONFIRMADO_REGIONAL<-split(testCASOS_CONFIRMADO_REGIONAL,by=c("CODIGO_COMUNA","ESCENARIOS"))
testCASOS_CONFIRMADO_REGIONAL<-parallel::parLapply(cl=clust,testCASOS_CONFIRMADO_REGIONAL,fun = funciones_ordenar_fecha_semana,tipo="FECHA")
testCASOS_CONFIRMADO_REGIONAL<-data.table::rbindlist(testCASOS_CONFIRMADO_REGIONAL)
testCASOS_CONFIRMADO_REGIONAL=testCASOS_CONFIRMADO_REGIONAL[, .SD[c(.N)], by=c("SEMANA","CODIGO_COMUNA","ESCENARIOS")][,c("SEMANA","CODIGO_COMUNA","ESCENARIOS",'DIMINUCION_SOTENIDA_2SEM','DIMINUCION_SOTENIDA_3SEM','TASA_PROYECTADA_FIT_50','TASA_PROYECTADA_FIT_25'),with=FALSE]

SEMANAS<-rep(SEMANAS,3)

for(i in unique(names(COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT)))
{
  X<-list(dat=COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT[[i]],pred_used=COVID_CASOS_CONFIRMADOS_COMUNAS_SPLIT_TS_MOD_PREDICT[[i]])
  testData[[i]]<-X
}

testData<-parallel::parLapply(cl=clust,X=testData,fun=funciones_umbral_velocidad_pred_parallel,nSemanas = nSemanas,PASO_COMUNAL=PASO_COMUNAL)


stopCluster(clust)
makePrediction<-TRUE
if(makePrediction){
for(i in unique(names(testData)))
{
  message(i)
  testData[[i]]=data.table::rbindlist(testData[[i]])
  testData[[i]][,SEMANA:=as.Date(SEMANA)]
  testData[[i]]<-testDataUCI[testData[[i]],on=c("ESCENARIOS","SEMANA")]
  testData[[i]]<-na.omit(testData[[i]])
  ESCENARIOS=testData[[i]]$ESCENARIOS
  SEMANAS=testData[[i]]$SEMANA
  testData[[i]]<-testDataUCI_REGIONAL[testData[[i]],on=c("ESCENARIOS","SEMANA","CODIGO_COMUNA")]
  testData[[i]]<-testPositividad_REGIONAL[testData[[i]],on=c("ESCENARIOS","SEMANA","CODIGO_COMUNA")]
  testData[[i]]<-testCASOS_CONFIRMADO_REGIONAL[testData[[i]],on=c("ESCENARIOS","SEMANA","CODIGO_COMUNA")]
  cols<-c('PASO','R_UMBRAL','DIMINUCION_SOTENIDA_2SEM','DIMINUCION_SOTENIDA_3SEM','TASA_PROYECTADA_FIT_50','TASA_PROYECTADA_FIT_25','FASE_1','FASE_2','FASE_3','FASE_4','FASE_REG_1','FASE_REG_2','FASE_REG_3','FASE_REG_4','FASE_POSITIVIDAD_1','FASE_POSITIVIDAD_2','FASE_POSITIVIDAD_3','FASE_POSITIVIDAD_4')
  cols_1<-c('PASO')
  cols_2<-c('R_UMBRAL','DIMINUCION_SOTENIDA_2SEM','DIMINUCION_SOTENIDA_3SEM','TASA_PROYECTADA_FIT_50','TASA_PROYECTADA_FIT_25','FASE_1','FASE_2','FASE_3','FASE_4','FASE_REG_1','FASE_REG_2','FASE_REG_3','FASE_REG_4','FASE_POSITIVIDAD_1','FASE_POSITIVIDAD_2','FASE_POSITIVIDAD_3','FASE_POSITIVIDAD_4')
  testData[[i]]<-testData[[i]][,cols,with=FALSE]
  testData[[i]][ , (cols_1) := lapply(.SD, factor,levels=c("1","2","3","4")), .SDcols = cols_1]
  testData[[i]][ , (cols_2) := lapply(.SD, factor,levels=c("TRUE","FALSE")), .SDcols = cols_2]
  
  #testData[[i]][ , (ckt) := lapply(.SD, function(x)return(x-1)), .SDcols = ckt]
  #pred[[i]] <- funciones_prediccion_modelo(mod=mod[[as.character(cluster[CODIGO_COMUNA ==i]$CLUSTER)]], testData=testData[[i]])
  pred[[i]] <- funciones_prediccion_modelo_dev(mod=mod[[as.character(cluster[CODIGO_COMUNA ==i]$CLUSTER)]], testData=testData[[i]])
  pred[[i]]<-data.table::setDT(as.data.frame(pred[[i]]))
  data.table::setnames(pred[[i]],old=c("-1","0","1"),new=c("bajar","mantenerse","subir"),skip_absent = TRUE)
  pred[[i]][,ESCENARIOS:=ESCENARIOS]
  pred[[i]][,SEMANAS:=SEMANAS]
  pred[[i]]<-pred[[i]][,c("bajar","mantenerse","subir","ESCENARIOS","SEMANAS"),with=FALSE]
  
}
  pred=Map(funciones_add_column, pred , names(pred))
  pred<-data.table::rbindlist(pred)
  data.table::setnames(pred,"name","CODIGO_COMUNA")
  pred_cast<-data.table::dcast(pred, CODIGO_COMUNA+SEMANAS  ~ ESCENARIOS,fun=base::mean,value.var=c("bajar","mantenerse","subir")) 
  data.table::setnames(pred_cast,"SEMANAS","FECHA")
  pred_cast<-split(pred_cast,by="CODIGO_COMUNA")
  pred_cast<-base::lapply(X=pred_cast,FUN=funciones_ordenar_fecha_semana,tipo="FECHA")
  pred_cast<-data.table::rbindlist(pred_cast)
  pred_cast[,SEMANA:=1:.N,by="CODIGO_COMUNA"]
  pred_cast[,KEY:=paste0(CODIGO_COMUNA,SEMANA),by=seq_len(nrow(pred_cast))]
  PASO_COMUNAL[,FECHA_MAX:=max(SEMANA,na.rm=TRUE)]
  PASO_ACTUAL=hana_get_complete_table(jdbcConnection=hanaConnection,nombre="GEOLOCALIZACION_FASE_ACTUAL_EMOL")[,c("CODIGO_COMUNA","PASO","PASO_PROB"),with=FALSE][,CODIGO_COMUNA:=as.character(CODIGO_COMUNA)]
  pred_cast<-PASO_ACTUAL[pred_cast,on="CODIGO_COMUNA"]
  pred_cast[PASO_PROB=="TRUE",c('subir_NORMAL','mantenerse_NORMAL','bajar_NORMAL','subir_OPTIMISTA','mantenerse_OPTIMISTA','bajar_OPTIMISTA','subir_PESIMISTA','mantenerse_PESIMISTA','bajar_PESIMISTA'):=list(0,1,0,0,1,0,0,1,0)]
  pred_cast<-pred_cast[,c('KEY','CODIGO_COMUNA','PASO','SEMANA','FECHA','subir_NORMAL','mantenerse_NORMAL','bajar_NORMAL','subir_OPTIMISTA','mantenerse_OPTIMISTA','bajar_OPTIMISTA','subir_PESIMISTA','mantenerse_PESIMISTA','bajar_PESIMISTA'),with=FALSE]
  data.table::setnames(pred_cast,"PASO", "Fase Actual")
  openxlsx::write.xlsx(pred_cast,paste0("output_pred_",Sys.Date(),"_.xlsx"))
}

end_time=Sys.time()
print(end_time-start_time)
