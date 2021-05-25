#0. SET WORKING DIRECTORY,CLEAN SCREEN, REMOVE VARIABLES----
setwd('~/covid-19')
cat("\014") 
graphics.off()
rm(list = ls())
options(java.parameters = "-Xmx8g")
options(encoding = 'UTF-8')
#install.packages("fMarkovSwitching", repos="http://R-Forge.R-project.org")
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
hanaConnection<-hana_connect()




fecha="2021-05-10"
resultadoDelModelo=data.table::setDT(openxlsx::read.xlsx(paste0("./Report/output_pred_",fecha,"_.xlsx")))
resultadoDelModelo[,CODIGO_COMUNA:=as.character(CODIGO_COMUNA)]
emol<-scraper_emol_table()
emol[,CODIGO_COMUNA:=as.character(CODIGO_COMUNA)]
analisis_cambios=resultadoDelModelo[SEMANA==1]

analisis_cambios<-emol[analisis_cambios,on="CODIGO_COMUNA"]
analisis_cambios[,RESULTADO:=PASO-Fase.Actual]


analisis_cambios_list<-split(analisis_cambios,by="RESULTADO")


analisis_cambios_list[['0']]<-analisis_cambios_list[['0']][,ACIERTO_DEL_MODELO:=any(c(mantenerse_NORMAL,mantenerse_PESIMISTA,mantenerse_OPTIMISTA)>=0.5) ,by=seq_len(nrow(analisis_cambios_list[['0']]))]


analisis_cambios_list[['1']]<-analisis_cambios_list[['1']][,ACIERTO_DEL_MODELO:=any(c(subir_NORMAL,subir_PESIMISTA,subir_OPTIMISTA)>=0.5) ,by=seq_len(nrow(analisis_cambios_list[['1']]))]
analisis_cambios_list[['-1']]<-analisis_cambios_list[['-1']][,ACIERTO_DEL_MODELO:=any(c(bajar_NORMAL,bajar_PESIMISTA,bajar_OPTIMISTA)>=0.5) ,by=seq_len(nrow(analisis_cambios_list[['-1']]))]


analisis<-data.table::rbindlist(analisis_cambios_list,fill=TRUE)

analisis[,Y_ESTIMADO:=c(1,0,-1)[which.min(c(subir_NORMAL,mantenerse_NORMAL,bajar_NORMAL))],by=seq_len(nrow(analisis))]

analisis<-na.omit(analisis)


acc=mean(analisis$ACIERTO_DEL_MODELO,na.rm = TRUE)

F1<-funciones_f1_score(predicted=analisis$Y_ESTIMADO, expected=analisis$RESULTADO, positive.class="1")

recall<-funciones_recall(predicted=analisis$Y_ESTIMADO, expected=analisis$RESULTADO, positive.class="1")

precision<-funciones_precision(predicted=analisis$Y_ESTIMADO, expected=analisis$RESULTADO, positive.class="1")


analisis[,c("acc","F1","recall","precision"):=list(acc,F1,recall,precision)]

openxlsx::write.xlsx(analisis,file=paste0("./AnalisisResultados/analisis_mod_pred_",fecha,"_.xlsx"))

