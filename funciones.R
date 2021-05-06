funciones_predict<-function(mod,n.ahead,CI=0.95)
{
  ### input ###
  # ts: objeto tipo serie temporal mensual,semanal, anual, diario
  # mod: modelo entrenad
  # CI: porcentaje de confianza del intervalo de prediccion
  ### output
  # acc: objeto forecast con la predicion y las medidas de adecuacion 
  forecast_out<-forecast::forecast(mod,h=n.ahead,level=CI)
  acc<-data.table::data.table()
  acc[,fit:=as.numeric(forecast_out$mean)]
  acc[,lower:=as.numeric(forecast_out$lower)]
  acc[,upper:=as.numeric(forecast_out$upper)]
  acc[,METODO:=sort(class(mod))[1]]
  acc[,c('ME','RMSE','MAE','MPE','MAPE','MASE','ACF1'):=as.list(forecast::accuracy(forecast_out))]
  return(acc)
}


funciones_create_ts<-function(dat,var,frecuencia)
{
  ### input ###
  # dat: objeto data frame
  # var: variable de estudio
  # frecuencia: temporabilidad de la variable Diaria o semanal
  ### output
  # ts: objeto tipo serie temporal
  if(frecuencia=="diaria")
  {
    dat<-unique(dat[,c("FECHA",var),with=FALSE])
    ts.series=as.vector(t(dat[as.vector(!is.na(dat[,var,with=FALSE]))][,var,with=FALSE]))
    fechas<-as.vector(t(dat[as.vector(!is.na(dat[,var,with=FALSE]))][,"FECHA",with=FALSE]))
    inds <- seq(as.Date(min(fechas)), as.Date(max(fechas)), by = "day")
    
    ts.series <- ts(ts.series, start = c(year(min(inds)), as.numeric(format(inds[1], "%j"))),
                    frequency = 365)
  }
  if(frecuencia=="semanal")
  {
    dat<-unique(dat[,c("SEMANA",var),with=FALSE])
    ts.series=as.vector(t(dat[as.vector(!is.na(dat[,var,with=FALSE]))][,var,with=FALSE]))
    fechas<-as.vector(t(dat[as.vector(!is.na(dat[,var,with=FALSE]))][,"SEMANA",with=FALSE]))
    ts.series <- ts(ts.series, frequency=52, start=c(year(min(fechas)),week(min(fechas)))) 
  }
  if(length(unique(ts.series[!is.na(ts.series)]))==1){return(NULL)}
  return(ts.series)
  
}

funciones_modelos_var<-function(ts.series)
{
  
  #modelos
  hw_object<-HoltWinters(ts.series,beta = TRUE, gamma = FALSE)
  arima_object<-auto.arima(ts.series)
  #struct_object <- StructTS(ts.series,"trend")
  modelos<-list(hw_object,arima_object)#,struct_object)
  return(modelos)
}

funciones_prediccion_var<-function(ts.series,n.ahead,CI=0.95)
{
  
  #modelos
  hw_object<-HoltWinters(ts.series,beta = TRUE, gamma = FALSE)
  arima_object<-auto.arima(ts.series)
  struct_object <- StructTS(ts.series,"trend")
  modelos<-list(hw_object,arima_object,struct_object)
  #Acc
  acc=lapply(X=modelos,FUN=funciones_predict,n.ahead=n.ahead,CI=CI)
  acc=data.table::rbindlist(acc)
  acc=acc[MAPE==min(MAPE)]
  acc=split(acc,by="METODO")[[1]]
  
  return(acc)
  
}

funciones_prediccion_var_mod<-function(modelos,n.ahead,CI)
{
  
  #Acc
  acc=lapply(X=modelos,FUN=funciones_predict,n.ahead=n.ahead,CI=CI)
  acc=data.table::rbindlist(acc)
  acc=acc[MAPE==min(MAPE)]
  acc=split(acc,by="METODO")[[1]]
  
  return(acc)
  
}




funciones_add_column <- function (data, name){
  data$name <- name
  data
}

funciones_download_csv_github<-function(product,mirror="MinCiencia/Datos-COVID19")
{
  html<-paste0("https://github.com/",mirror,"/tree/master/output/",product)
  nombre_archivo<-scraper_files_csv(html)
  if(length(nombre_archivo)>0)
  {
    files<-paste0("https://raw.githubusercontent.com/",mirror,"/master/output/",product,"/",nombre_archivo)
    fread_latin<-function(x){dat=data.table::fread(x,encoding="Latin-1");return(dat)}
    out<-lapply(files,fread_latin)
    names(out)<-nombre_archivo
    out<-Map(funciones_add_column, out , names(out))
  }else{
    out<-NULL
  }
  
  return(out)
}

funciones_plot_pred<-function(ts_object,mod_object,error.ribbon='green',n.ahead,CI, line.size=1,ylabel,xlabel){
  forecast<-predict(mod_object,  n.ahead=n.ahead,  prediction.interval=T,  level=CI)
  for_values<-data.frame(time=round(time(forecast),  3),  value_forecast=as.data.frame(forecast)$fit,  dev=as.data.frame(forecast)$upr-as.data.frame(forecast)$fit)
  fitted_values<-data.frame(time=round(time(mod_object$fitted),  3),  value_fitted=as.data.frame(mod_object$fitted)$xhat)
  actual_values<-data.frame(time=round(time(mod_object$x),  3),  Actual=c(mod_object$x))
  graphset<-merge(actual_values,  fitted_values,  by='time',  all=TRUE)
  graphset<-merge(graphset,  for_values,  all=TRUE,  by='time')
  graphset[is.na(graphset$dev),  ]$dev<-0
  graphset$Fitted<-c(rep(NA,  NROW(graphset)-(NROW(for_values) + NROW(fitted_values))),  fitted_values$value_fitted,  for_values$value_forecast)
  
  graphset.melt<-reshape2::melt(graphset[, c('time', 'Actual', 'Fitted')], id='time')
  
  p<-ggplot(graphset.melt,  aes(x=time,  y=value)) + geom_ribbon(data=graphset, aes(x=time, y=Fitted, ymin=Fitted-dev,  ymax=Fitted + dev),  alpha=.2,  fill=error.ribbon) + geom_line(aes(colour=variable), size=line.size) + geom_vline(xintercept=max(actual_values$time),  lty=2) + xlab(xlabel) + ylab(ylabel)+ scale_colour_hue('')
  return(p)
}
funciones_umbral_casos_pred<-function(dat,pred_used){
  
  estimacion<-as.vector(t(pred_used[,c("fit","lower","upper"),with=FALSE]))
  escenarios<-c("NORMAL","OPTIMISTA","PESIMISTA")
  out<-data.table::data.table(ESCENARIOS=escenarios,CODIGO_COMUNA=unique(dat$CODIGO_COMUNA),POBLACION=unique(dat$POBLACION),CASOS_ACTUALES_FIN=estimacion,SEMANA=as.character(as.Date(max(dat$SEMANA))+7))
  
  
  dat_optimista=dat[,c("CODIGO_COMUNA","POBLACION","SEMANA","CASOS_ACTUALES_FIN"),with=FALSE]
  dat_optimista[,ESCENARIOS:="OPTIMISTA"]
  dat_optimista<-data.table::rbindlist(list(dat_optimista,out[ESCENARIOS=="OPTIMISTA"]),use.names=TRUE)
  
  dat_pesimista=dat[,c("CODIGO_COMUNA","POBLACION","SEMANA","CASOS_ACTUALES_FIN"),with=FALSE]
  dat_pesimista[,ESCENARIOS:="PESIMISTA"]
  dat_pesimista<-data.table::rbindlist(list(dat_pesimista,out[ESCENARIOS=="PESIMISTA"]),use.names=TRUE)
  
  dat_normal=dat[,c("CODIGO_COMUNA","POBLACION","SEMANA","CASOS_ACTUALES_FIN"),with=FALSE]
  dat_normal[,ESCENARIOS:="NORMAL"]
  dat_normal<-data.table::rbindlist(list(dat_normal,out[ESCENARIOS=="NORMAL"]),use.names=TRUE)
  dat_estudio<-split(data.table::rbindlist(list(dat_optimista,dat_pesimista,dat_normal)),by="ESCENARIOS")
  
  out=base::lapply(X=dat_estudio,FUN=funciones_umbral_casos)
  out=base::lapply(X=out,FUN=utils::tail,n=nrow(pred_used))
  
  
  return(out)
  
}


funciones_umbral_casos_pred_regiones<-function(dat,pred_used){
  
  
  cols=c("CODIGO_REGION","POBLACION","FECHA","CASOS_CONFIRMADO_REGION")
  
  estimacion<-as.vector(t(pred_used[,c("fit","lower","upper"),with=FALSE]))
  escenarios<-c("NORMAL","OPTIMISTA","PESIMISTA")
  out<-data.table::data.table(ESCENARIOS=escenarios,CODIGO_REGION=unique(dat$CODIGO_REGION),POBLACION=unique(dat$POBLACION),CASOS_CONFIRMADO_REGION=estimacion,FECHA=as.character(as.Date(max(dat$FECHA))+nrow(pred_used)))
  
  
  dat_optimista=dat[,cols,with=FALSE]
  dat_optimista[,ESCENARIOS:="OPTIMISTA"]
  dat_optimista<-data.table::rbindlist(list(dat_optimista,out[ESCENARIOS=="OPTIMISTA"]),use.names=TRUE)
  
  dat_pesimista=dat[,cols,with=FALSE]
  dat_pesimista[,ESCENARIOS:="PESIMISTA"]
  dat_pesimista<-data.table::rbindlist(list(dat_pesimista,out[ESCENARIOS=="PESIMISTA"]),use.names=TRUE)
  
  dat_normal=dat[,cols,with=FALSE]
  dat_normal[,ESCENARIOS:="NORMAL"]
  dat_normal<-data.table::rbindlist(list(dat_normal,out[ESCENARIOS=="NORMAL"]),use.names=TRUE)
  
  dat_estudio<-split(data.table::rbindlist(list(dat_optimista,dat_pesimista,dat_normal)),by="ESCENARIOS")
  
  out=base::lapply(X=dat_estudio,FUN=funciones_umbral_casos_region)
  out=base::lapply(X=out,FUN=utils::tail,n=nrow(pred_used))
  
  
  return(out)
  
}


funciones_umbral_casos_pred_regiones_parallel<-function(X,regionComuna){
  
  dat<-X$dat
  pred_used<-X$pred_used
  cols=c("CODIGO_REGION","POBLACION","FECHA","SEMANA","CASOS_CONFIRMADO_REGION")
  pred_used[,FECHA:=seq(from=as.Date(max(dat$FECHA))+1, by="days", length.out = nrow(pred_used))]
  data.table::setnames(pred_used,c("fit","lower","upper"),c("NORMAL","OPTIMISTA","PESIMISTA"))
  
  out<-melt(pred_used, id.vars="FECHA", measure.vars=c("NORMAL","OPTIMISTA","PESIMISTA"),variable.name = "ESCENARIOS", value.name = "CASOS_CONFIRMADO_REGION")
  out[,c("CODIGO_REGION","POBLACION"):=list(unique(dat$CODIGO_REGION),unique(dat$POBLACION))]
  
  
  
  
  out[, SEMANA:= as.Date(unique(cut(as.Date(FECHA), "week"))),by=seq_len(nrow(out))]
  dat_optimista=dat[,cols,with=FALSE]
  dat_optimista[,ESCENARIOS:="OPTIMISTA"]
  dat_optimista<-data.table::rbindlist(list(dat_optimista,out[ESCENARIOS=="OPTIMISTA"]),use.names=TRUE)
  
  dat_pesimista=dat[,cols,with=FALSE]
  dat_pesimista[,ESCENARIOS:="PESIMISTA"]
  dat_pesimista<-data.table::rbindlist(list(dat_pesimista,out[ESCENARIOS=="PESIMISTA"]),use.names=TRUE)
  
  dat_normal=dat[,cols,with=FALSE]
  dat_normal[,ESCENARIOS:="NORMAL"]
  dat_normal<-data.table::rbindlist(list(dat_normal,out[ESCENARIOS=="NORMAL"]),use.names=TRUE)
  dat_estudio<-split(data.table::rbindlist(list(dat_optimista,dat_pesimista,dat_normal)),by="ESCENARIOS")
  
  out=base::lapply(X=dat_estudio,FUN=funciones_umbral_casos_region)
  out=base::lapply(X=out,FUN=utils::tail,n=nrow(pred_used))
  
  out=Map(funciones_add_column, out , names(out))
  out=data.table::rbindlist(out)
  data.table::setnames(out,old="name",new="ESCENARIOS")
  out=regionComuna[out,on="CODIGO_REGION",allow.cartesian=TRUE]
  
  
  out<-out[,c("FECHA","SEMANA","CODIGO_COMUNA","ESCENARIOS",'DIMINUCION_SOTENIDA_2SEM','DIMINUCION_SOTENIDA_3SEM','TASA_PROYECTADA_FIT_50','TASA_PROYECTADA_FIT_25')]
  out[,FECHA:=as.Date(FECHA)]
  out<-unique(out)
  
  
  return(out)
  
}


funciones_dinamica_contagios<-function(dat,territorio="CODIGO_COMUNA"){
  
  dat=unique(dat[,c(territorio,"POBLACION","FECHA","CASOS"),with=FALSE])
  marco<-data.table::data.table(FECHA=as.Date(seq(as.Date(min(dat$FECHA)),as.Date(max(dat$FECHA)),by="days")))
  dat=dat[marco,on="FECHA"]
  
  dat[ , (names(dat)) := lapply(.SD, zoo::na.locf), .SDcols = names(dat)]
  
  dat[,PM7:=zoo::rollmean(CASOS, k = 7,fill = NA,align = "right")]
  dat[,casosAcumulados:=cumsum(CASOS)]
  dat[,fila:=1:.N]
  dat[,PM7:=ifelse(is.na(PM7),casosAcumulados/fila,PM7)]
  dat[,C:=100000*(PM7/POBLACION),by=seq_len(nrow(dat))]
  dat=unique(dat[,c(territorio,"POBLACION","FECHA","CASOS","C","PM7"),with=FALSE])
  dat<-na.omit(dat)
  dat<-utils::tail(dat,38)
  
  
  incidencias<-dat[,c("FECHA","CASOS"),with=FALSE]
  data.table::setnames(incidencias,c("FECHA","CASOS"),c("dates","I"))
  incidencias[,dates:=as.Date(dates)]
  incidencias<-utils::tail(incidencias,38)
  incidencias[,I:=ifelse(I<0,0,I)]
  config <- make_config(list(mean_si = 5, std_mean_si = 1,
                             min_mean_si = 3, max_mean_si = 7,
                             std_si = 3.8, std_std_si = 0.5,
                             min_std_si = 1.8, max_std_si = 5.8))
  res_parametric_si <- EpiEstim::estimate_R(incidencias, 
                                            #method="uncertain_si",
                                            method="parametric_si",
                                            config = config)
  res<-data.table::setDT(res_parametric_si$R)
  res[,FECHA:=dat$FECHA[res$t_end]]
  res=res[,c('Quantile.0.025(R)','Median(R)','Quantile.0.975(R)','FECHA'),with=FALSE]
  dat<-res[dat,on="FECHA"]
  dat<-na.omit(dat)
  dat<-dat[,c('FECHA',territorio,'POBLACION','CASOS','C','PM7','Quantile.0.025(R)','Median(R)','Quantile.0.975(R)'),with=FALSE]
  dat[,R_UMBRAL:=`Median(R)`<1]
  return(dat)
}

funciones_dinamica_contagios_train<-function(dat,territorio="CODIGO_COMUNA"){
  
  dat=unique(dat[,c(territorio,"POBLACION","FECHA","SEMANA","CASOS"),with=FALSE])
  marco<-data.table::data.table(FECHA=as.Date(seq(as.Date(min(dat$FECHA)),as.Date(max(dat$FECHA)),by="days")))
  dat=dat[marco,on="FECHA"]
  
  dat[ , (names(dat)) := lapply(.SD, zoo::na.locf), .SDcols = names(dat)]
  
  dat[,PM7:=zoo::rollmean(CASOS, k = 7,fill = NA,align = "right")]
  dat[,casosAcumulados:=cumsum(CASOS)]
  dat[,fila:=1:.N]
  dat[,PM7:=ifelse(is.na(PM7),casosAcumulados/fila,PM7)]
  dat[,C:=100000*(PM7/POBLACION),by=seq_len(nrow(dat))]
  dat=unique(dat[,c(territorio,"POBLACION","FECHA","SEMANA","CASOS","C","PM7"),with=FALSE])
  dat<-na.omit(dat)
  #dat<-utils::tail(dat,38)
  
  
  incidencias<-dat[,c("FECHA","CASOS"),with=FALSE]
  data.table::setnames(incidencias,c("FECHA","CASOS"),c("dates","I"))
  incidencias[,dates:=as.Date(dates)]
  #incidencias<-utils::tail(incidencias,38)
  incidencias[,I:=ifelse(I<0,0,I)]
  config <- make_config(list(mean_si = 5, std_mean_si = 1,
                             min_mean_si = 3, max_mean_si = 7,
                             std_si = 3.8, std_std_si = 0.5,
                             min_std_si = 1.8, max_std_si = 5.8))
  res_parametric_si <- EpiEstim::estimate_R(incidencias, 
                                            #method="uncertain_si",
                                            method="parametric_si",
                                            config = config)
  res<-data.table::setDT(res_parametric_si$R)
  res[,FECHA:=dat$FECHA[res$t_end]]
  res=res[,c('Quantile.0.025(R)','Median(R)','Quantile.0.975(R)','FECHA'),with=FALSE]
  dat<-res[dat,on="FECHA"]
  dat<-na.omit(dat)
  dat<-dat[,c('FECHA','SEMANA',territorio,'POBLACION','CASOS','C','PM7','Quantile.0.025(R)','Median(R)','Quantile.0.975(R)'),with=FALSE]
  dat[,R_UMBRAL:=`Median(R)`<1]
  return(dat)
}

funciones_umbral_casos<-function(dat){
  dat=dat[,c("CODIGO_COMUNA","POBLACION","SEMANA","CASOS_ACTUALES_FIN"),with=FALSE]
  dat[,TASA_PROYECTADA:=100000*(CASOS_ACTUALES_FIN/POBLACION),by=seq_len(nrow(dat))]
  dat[,"TASA_PROYECTADA_MEAN":=zoo::rollmean(TASA_PROYECTADA, k = 7,fill = NA,align = "right")]
  dat<-na.omit(dat)
  cols="TASA_PROYECTADA_MEAN"
  dat[, ("CASOS_ACTUALES_FIN_1") := shift(.SD, 1, 0, "lag"), .SDcols=cols]
  dat[, ("CASOS_ACTUALES_FIN_2") := shift(.SD, 2, 0, "lag"), .SDcols=cols]
  
  dat[,DIMINUCION_SOTENIDA_2SEM:=all((CASOS_ACTUALES_FIN-CASOS_ACTUALES_FIN_1)<0),by=seq_len(nrow(dat))]
  dat[,DIMINUCION_SOTENIDA_3SEM:=all(c(CASOS_ACTUALES_FIN-CASOS_ACTUALES_FIN_1,CASOS_ACTUALES_FIN_1-CASOS_ACTUALES_FIN_2)<0),by=seq_len(nrow(dat))]
  dat[,TASA_PROYECTADA_FIT_50:=TASA_PROYECTADA<=50]
  dat[,TASA_PROYECTADA_FIT_25:=TASA_PROYECTADA<=25]
  
  return(dat)
  
}

funciones_umbral_resp<-function(dat){
  dat=dat[,c("CODIGO_COMUNA","POBLACION","SEMANA","CASOS_ACTUALES_FIN"),with=FALSE]
  cols="CASOS_ACTUALES_FIN"
  dat[, ("CASOS_ACTUALES_FIN_1") := shift(.SD, 1, 0, "lag"), .SDcols=cols]
  dat[, ("CASOS_ACTUALES_FIN_2") := shift(.SD, 2, 0, "lag"), .SDcols=cols]
  dat[,TASA_PROYECTADA:=100000*(CASOS_ACTUALES_FIN_1/POBLACION),by=seq_len(nrow(dat))]
  dat[,DIMINUCION_SOTENIDA_2SEM:=all((CASOS_ACTUALES_FIN-CASOS_ACTUALES_FIN_1)<0),by=seq_len(nrow(dat))]
  dat[,DIMINUCION_SOTENIDA_3SEM:=all(c(CASOS_ACTUALES_FIN-CASOS_ACTUALES_FIN_1,CASOS_ACTUALES_FIN_1-CASOS_ACTUALES_FIN_2)<0),by=seq_len(nrow(dat))]
  dat[,TASA_PROYECTADA_FIT_50:=TASA_PROYECTADA<=50]
  dat[,TASA_PROYECTADA_FIT_25:=TASA_PROYECTADA<=25]
  
  return(dat)
  
}

funciones_umbral_casos_region<-function(dat){
  dat=dat[,c("CODIGO_REGION","POBLACION","FECHA","SEMANA","CASOS_CONFIRMADO_REGION"),with=FALSE]
  dat[,TASA_PROYECTADA:=100000*(CASOS_CONFIRMADO_REGION/POBLACION),by=seq_len(nrow(dat))]
  dat[,"TASA_PROYECTADA_MEAN":=zoo::rollmean(TASA_PROYECTADA, k = 7,fill = NA,align = "right")]
  dat=na.omit(dat)
  cols="TASA_PROYECTADA_MEAN"
  dat[, ("CASOS_ACTUALES_FIN_1") := shift(.SD, 1, 0, "lag"), .SDcols=cols]
  dat[, ("CASOS_ACTUALES_FIN_2") := shift(.SD, 2, 0, "lag"), .SDcols=cols]
  
  dat[,DIMINUCION_SOTENIDA_2SEM:=all((TASA_PROYECTADA_MEAN-CASOS_ACTUALES_FIN_1)<0),by=seq_len(nrow(dat))]
  dat[,DIMINUCION_SOTENIDA_3SEM:=all(c(TASA_PROYECTADA_MEAN-CASOS_ACTUALES_FIN_1,CASOS_ACTUALES_FIN_1-CASOS_ACTUALES_FIN_2)<0),by=seq_len(nrow(dat))]
  dat[,TASA_PROYECTADA_FIT_50:=TASA_PROYECTADA_MEAN<=50]
  dat[,TASA_PROYECTADA_FIT_25:=TASA_PROYECTADA_MEAN<=25]
  
  return(dat)
  
}


funciones_umbral_casos_uci<-function(dat,umbral){
  
  dat=unique(dat[,c("FECHA","SEMANA","REGION_RESIDENCIA","CAMAS_UCI_OCUPADAS_COVID_19","CAMAS_UCI_OCUPADAS_NO_COVID_19","CAMAS_UCI_HABILITADAS")])
  dat[,PORCENTAJE_DE_OCUPACION:=100*(CAMAS_UCI_OCUPADAS_COVID_19+CAMAS_UCI_OCUPADAS_NO_COVID_19)/CAMAS_UCI_HABILITADAS]
  dat[,c("UMBRAL_1","UMBRAL_2","UMBRAL_3","UMBRAL_4"):=as.list(umbral)]
  dat[,c("FASE_1","FASE_2","FASE_3","FASE_4"):=list(UMBRAL_1>PORCENTAJE_DE_OCUPACION ,UMBRAL_2>PORCENTAJE_DE_OCUPACION, UMBRAL_3>PORCENTAJE_DE_OCUPACION, UMBRAL_4>PORCENTAJE_DE_OCUPACION),by=seq_len(nrow(dat))]
  return(dat)
}



funciones_umbral_positividad<-function(dat){
  umbral<-c(15,10,10,5)
  dat=unique(dat[,c("FECHA","SEMANA","REGION_RESIDENCIA","CASOS_NUEVOS_TOTALES","NUMERO_PCR")])
  dat[,POSITIVIDAD:=100*CASOS_NUEVOS_TOTALES/NUMERO_PCR]
  dat[,c("UMBRAL_1","UMBRAL_2","UMBRAL_3","UMBRAL_4"):=as.list(umbral)]
  dat[,c("FASE_POSITIVIDAD_1","FASE_POSITIVIDAD_2","FASE_POSITIVIDAD_3","FASE_POSITIVIDAD_4"):=list(UMBRAL_1>POSITIVIDAD ,UMBRAL_2>POSITIVIDAD, UMBRAL_3>POSITIVIDAD, UMBRAL_4>POSITIVIDAD),by=seq_len(nrow(dat))]
  return(dat)
}



funciones_umbral_positividad_pred<-function(dat,pred_pcr,pred_casos){
  
  pred_pcr[,FECHA:=as.character(seq.Date(as.Date(max(dat$FECHA))+1,by="days",length.out = nrow(pred_pcr)))]
  pred_pcr[,c("REGION_RESIDENCIA"):=list(unique(dat$REGION_RESIDENCIA))]
  pred_pcr=pred_pcr[,c( "FECHA","REGION_RESIDENCIA","fit","lower","upper"),with=FALSE]
  data.table::setnames(pred_pcr,c("fit","lower","upper"),c("NORMAL","OPTIMISTA","PESIMISTA"),skip_absent = TRUE)
  pred_pcr_melt<-data.table::melt(pred_pcr,id.vars=c("FECHA","REGION_RESIDENCIA"), measure.vars=c("NORMAL","OPTIMISTA","PESIMISTA"),
                                  variable.name = "ESCENARIO", value.name = "NUMERO_PCR")
  
  
  pred_casos[,FECHA:=as.character(seq.Date(as.Date(max(dat$FECHA))+1,by="days",length.out = nrow(pred_casos)))]
  pred_casos[,c("REGION_RESIDENCIA"):=list(unique(dat$REGION_RESIDENCIA))]
  pred_casos=pred_casos[,c( "FECHA","REGION_RESIDENCIA","fit","lower","upper"),with=FALSE]
  data.table::setnames(pred_casos,c("fit","lower","upper"),c("NORMAL","OPTIMISTA","PESIMISTA"),skip_absent = TRUE)
  pred_casos_melt<-data.table::melt(pred_casos,id.vars=c("FECHA","REGION_RESIDENCIA"), measure.vars=c("NORMAL","OPTIMISTA","PESIMISTA"),
                                    variable.name = "ESCENARIO", value.name = "CASOS_NUEVOS_TOTALES")
  
  pred_used=pred_casos_melt[pred_pcr_melt,on=c("FECHA","REGION_RESIDENCIA","ESCENARIO")]
  pred_used_split<-split(pred_used,by="ESCENARIO")
  cols_1=c("CASOS_NUEVOS_TOTALES","NUMERO_PCR")
  cols_2="ESCENARIO"
  
  dat=unique(dat[,c("FECHA","REGION_RESIDENCIA","CASOS_NUEVOS_TOTALES","NUMERO_PCR")])
  for(i in 1:length(pred_used_split)){
    pred_used_split[[i]]<-data.table::rbindlist(list(dat,pred_used_split[[i]]),fill = TRUE)
    pred_used_split[[i]][, (cols_1) := lapply(.SD,zoo::na.locf), .SDcols=cols_1]
    pred_used_split[[i]][, (cols_2) := NULL]
    pred_used_split[[i]][, SEMANA:= as.Date(unique(cut(as.Date(FECHA), "week"))),by=seq_len(nrow(pred_used_split[[i]]))]
    pred_used_split[[i]]=pred_used_split[[i]][, .SD[c(.N)], by="SEMANA"]
  }
  pred_used_split_out<-base::lapply(pred_used_split,funciones_ordenar_fecha_semana,tipo="SEMANA")
  pred_used_split_out<-base::lapply(pred_used_split,funciones_umbral_positividad)
  out=base::lapply(X=pred_used_split_out,FUN=utils::tail,n=nrow(pred_used))
  for(j in 1:length(out)){
    
    out[[j]][, SEMANA:= as.Date(unique(cut(as.Date(FECHA), "week"))),by=seq_len(nrow(out[[i]]))]
    
  }
  return(out)
}

funciones_umbral_positividad_pred_parallel<-function(X,regionComuna){
  dat=X$dat
  pred_pcr=X$pred_pcr
  pred_casos=X$pred_casos
  pred_pcr[,FECHA:=as.Date(seq.Date(as.Date(max(dat$FECHA))+1,by="days",length.out = nrow(pred_pcr)))]
  pred_pcr[,c("REGION_RESIDENCIA"):=list(unique(dat$REGION_RESIDENCIA))]
  pred_pcr=pred_pcr[,c( "FECHA","REGION_RESIDENCIA","fit","lower","upper"),with=FALSE]
  data.table::setnames(pred_pcr,c("fit","lower","upper"),c("NORMAL","OPTIMISTA","PESIMISTA"),skip_absent = TRUE)
  pred_pcr_melt<-data.table::melt(pred_pcr,id.vars=c("FECHA","REGION_RESIDENCIA"), measure.vars=c("NORMAL","OPTIMISTA","PESIMISTA"),
                                  variable.name = "ESCENARIO", value.name = "NUMERO_PCR")
  
  
  pred_casos[,FECHA:=as.Date(seq.Date(as.Date(max(dat$FECHA))+1,by="days",length.out = nrow(pred_casos)))]
  pred_casos[,c("REGION_RESIDENCIA"):=list(unique(dat$REGION_RESIDENCIA))]
  pred_casos=pred_casos[,c( "FECHA","REGION_RESIDENCIA","fit","lower","upper"),with=FALSE]
  data.table::setnames(pred_casos,c("fit","lower","upper"),c("NORMAL","OPTIMISTA","PESIMISTA"),skip_absent = TRUE)
  pred_casos_melt<-data.table::melt(pred_casos,id.vars=c("FECHA","REGION_RESIDENCIA"), measure.vars=c("NORMAL","OPTIMISTA","PESIMISTA"),
                                    variable.name = "ESCENARIO", value.name = "CASOS_NUEVOS_TOTALES")
  
  pred_used=pred_casos_melt[pred_pcr_melt,on=c("FECHA","REGION_RESIDENCIA","ESCENARIO")]
  pred_used[, SEMANA:= as.Date(unique(cut(as.Date(FECHA), "week"))),by=seq_len(nrow(pred_used))]
  pred_used_split<-split(pred_used,by="ESCENARIO")
  cols_1=c("CASOS_NUEVOS_TOTALES","NUMERO_PCR")
  cols_2="ESCENARIO"
  
  dat=unique(dat[,c("FECHA","SEMANA","REGION_RESIDENCIA","CASOS_NUEVOS_TOTALES","NUMERO_PCR")])
  for(i in 1:length(pred_used_split)){
    pred_used_split[[i]]<-data.table::rbindlist(list(dat,pred_used_split[[i]]),fill = TRUE)
    pred_used_split[[i]][, (cols_1) := lapply(.SD,zoo::na.locf), .SDcols=cols_1]
    pred_used_split[[i]][, (cols_2) := NULL]
    pred_used_split[[i]]=pred_used_split[[i]][, .SD[c(.N)], by="SEMANA"]
  }
  pred_used_split_out<-base::lapply(pred_used_split,funciones_ordenar_fecha_semana,tipo="SEMANA")
  pred_used_split_out<-base::lapply(pred_used_split,funciones_umbral_positividad)
  out=base::lapply(X=pred_used_split_out,FUN=utils::tail,n=nrow(pred_used))
  
  out=Map(funciones_add_column, out , names(out))
  out=data.table::rbindlist(out)
  data.table::setnames(out,old="name",new="ESCENARIOS")
  out=regionComuna[out,on="REGION_RESIDENCIA",allow.cartesian=TRUE]
  
  out<-out[,c("FECHA","SEMANA","CODIGO_COMUNA","ESCENARIOS","FASE_POSITIVIDAD_1","FASE_POSITIVIDAD_2","FASE_POSITIVIDAD_3","FASE_POSITIVIDAD_4")]
  out[,FECHA:=as.Date(FECHA)]
  out<-unique(out)
  return(out)
}

funciones_umbral_casos_uci_pred<-function(dat,pred_used_covid,pred_used_no_covid,umbral){
  
  N<-nrow(pred_used_covid)
  pred_used_covid[,FECHA:=as.Date(seq.Date(as.Date(max(dat$FECHA))+1,by="days",length.out = nrow(pred_used_covid)))]
  pred_used_covid[,c("REGION_RESIDENCIA"):=list(unique(dat$REGION_RESIDENCIA))]
  pred_used_covid=pred_used_covid[,c( "FECHA","REGION_RESIDENCIA","fit","lower","upper"),with=FALSE]
  data.table::setnames(pred_used_covid,c("fit","lower","upper"),c("NORMAL","OPTIMISTA","PESIMISTA"),skip_absent = TRUE)
  pred_used_covid_melt<-data.table::melt(pred_used_covid,id.vars=c("FECHA","REGION_RESIDENCIA"), measure.vars=c("NORMAL","OPTIMISTA","PESIMISTA"),
                                         variable.name = "ESCENARIO", value.name = "CAMAS_UCI_OCUPADAS_COVID_19")
  
  
  pred_used_no_covid[,FECHA:=as.Date(seq.Date(as.Date(max(dat$FECHA))+1,by="days",length.out = nrow(pred_used_no_covid)))]
  pred_used_no_covid[,c("REGION_RESIDENCIA"):=list(unique(dat$REGION_RESIDENCIA))]
  pred_used_no_covid=pred_used_no_covid[,c( "FECHA","REGION_RESIDENCIA","fit","lower","upper"),with=FALSE]
  data.table::setnames(pred_used_no_covid,c("fit","lower","upper"),c("NORMAL","OPTIMISTA","PESIMISTA"),skip_absent = TRUE)
  pred_used_no_covid_melt<-data.table::melt(pred_used_no_covid,id.vars=c("FECHA","REGION_RESIDENCIA"), measure.vars=c("NORMAL","OPTIMISTA","PESIMISTA"),
                                            variable.name = "ESCENARIO", value.name = "CAMAS_UCI_OCUPADAS_NO_COVID_19")
  
  pred_used=pred_used_no_covid_melt[pred_used_covid_melt,on=c("FECHA","REGION_RESIDENCIA","ESCENARIO")]
  pred_used_split<-split(pred_used,by="ESCENARIO")
  cols_1="CAMAS_UCI_HABILITADAS"
  cols_2="ESCENARIO"
  
  dat=unique(dat[,c("FECHA","REGION_RESIDENCIA","SEMANA","CAMAS_UCI_OCUPADAS_COVID_19","CAMAS_UCI_OCUPADAS_NO_COVID_19","CAMAS_UCI_HABILITADAS")])
  for(i in 1:length(pred_used_split)){
    pred_used_split[[i]]<-data.table::rbindlist(list(dat,pred_used_split[[i]]),fill = TRUE)
    pred_used_split[[i]][, (cols_1) := lapply(.SD,zoo::na.locf), .SDcols=cols_1]
    pred_used_split[[i]][, (cols_2) := NULL]
    pred_used_split[[i]][, SEMANA:=ifelse(is.na(SEMANA),as.Date(unique(cut(as.Date(FECHA), "week"))),SEMANA),by=seq_len(nrow(pred_used_split[[i]]))]
    pred_used_split[[i]]=pred_used_split[[i]][, .SD[c(.N)], by="SEMANA"]
  }
  pred_used_split_out<-base::lapply(pred_used_split,funciones_ordenar_fecha_semana,tipo="SEMANA")
  pred_used_split_out<-base::lapply(pred_used_split,funciones_umbral_casos_uci,umbral=umbral)
  out=base::lapply(X=pred_used_split_out,FUN=utils::tail,n=N)
  return(out)
}


funciones_umbral_casos_uci_pred_parallel<-function(X,umbral,regionComuna){
  
  dat=X$dat
  pred_used_covid=X$pred_used_covid
  pred_used_no_covid=X$pred_used_no_covid
  
  
  pred_used_covid[,FECHA:=as.Date(seq.Date(as.Date(max(dat$FECHA))+1,by="days",length.out = nrow(pred_used_covid)))]
  pred_used_covid[,c("REGION_RESIDENCIA"):=list(unique(dat$REGION_RESIDENCIA))]
  pred_used_covid=pred_used_covid[,c( "FECHA","REGION_RESIDENCIA","fit","lower","upper"),with=FALSE]
  data.table::setnames(pred_used_covid,c("fit","lower","upper"),c("NORMAL","OPTIMISTA","PESIMISTA"),skip_absent = TRUE)
  pred_used_covid_melt<-data.table::melt(pred_used_covid,id.vars=c("FECHA","REGION_RESIDENCIA"), measure.vars=c("NORMAL","OPTIMISTA","PESIMISTA"),
                                         variable.name = "ESCENARIO", value.name = "CAMAS_UCI_OCUPADAS_COVID_19")
  
  
  pred_used_no_covid[,FECHA:=as.Date(seq.Date(as.Date(max(dat$FECHA))+1,by="days",length.out = nrow(pred_used_no_covid)))]
  pred_used_no_covid[,c("REGION_RESIDENCIA"):=list(unique(dat$REGION_RESIDENCIA))]
  pred_used_no_covid=pred_used_no_covid[,c( "FECHA","REGION_RESIDENCIA","fit","lower","upper"),with=FALSE]
  data.table::setnames(pred_used_no_covid,c("fit","lower","upper"),c("NORMAL","OPTIMISTA","PESIMISTA"),skip_absent = TRUE)
  pred_used_no_covid_melt<-data.table::melt(pred_used_no_covid,id.vars=c("FECHA","REGION_RESIDENCIA"), measure.vars=c("NORMAL","OPTIMISTA","PESIMISTA"),
                                            variable.name = "ESCENARIO", value.name = "CAMAS_UCI_OCUPADAS_NO_COVID_19")
  
  pred_used=pred_used_no_covid_melt[pred_used_covid_melt,on=c("FECHA","REGION_RESIDENCIA","ESCENARIO")]
  pred_used_split<-split(pred_used,by="ESCENARIO")
  cols_1="CAMAS_UCI_HABILITADAS"
  cols_2="ESCENARIO"
  
  dat=unique(dat[,c("FECHA","SEMANA","REGION_RESIDENCIA","CAMAS_UCI_OCUPADAS_COVID_19","CAMAS_UCI_OCUPADAS_NO_COVID_19","CAMAS_UCI_HABILITADAS")])
  for(i in 1:length(pred_used_split)){
    pred_used_split[[i]]<-data.table::rbindlist(list(dat,pred_used_split[[i]]),fill = TRUE)
    pred_used_split[[i]][, SEMANA:=ifelse(is.na(SEMANA),as.Date(unique(cut(as.Date(FECHA), "week"))),SEMANA),by=seq_len(nrow(pred_used_split[[i]]))]
    pred_used_split[[i]][, (cols_1) := lapply(.SD,zoo::na.locf), .SDcols=cols_1]
    pred_used_split[[i]][, (cols_2) := NULL]
    pred_used_split[[i]]=pred_used_split[[i]][, .SD[c(.N)], by="SEMANA"]
  }
  pred_used_split_out<-base::lapply(pred_used_split,funciones_ordenar_fecha_semana,tipo="SEMANA")
  pred_used_split_out<-base::lapply(pred_used_split,funciones_umbral_casos_uci,umbral=umbral)
  out=base::lapply(X=pred_used_split_out,FUN=utils::tail,n=nrow(pred_used))

  out=Map(funciones_add_column, out , names(out))
  out=data.table::rbindlist(out)
  data.table::setnames(out,old="name",new="ESCENARIOS")
  out=regionComuna[out,on="REGION_RESIDENCIA",allow.cartesian=TRUE]
  data.table::setnames(out,old=c("FASE_1","FASE_2","FASE_3","FASE_4"),new=c("FASE_REG_1","FASE_REG_2","FASE_REG_3","FASE_REG_4"),skip_absent = TRUE)
  
  out<-out[,c("FECHA","SEMANA","CODIGO_COMUNA","ESCENARIOS","FASE_REG_1","FASE_REG_2","FASE_REG_3","FASE_REG_4")]
  out[,FECHA:=as.Date(FECHA)]
  out<-unique(out)
  return(out)
}



funciones_fill_na<-function(DT)
{
  for (i in names(DT))
    DT[is.na(get(i)), (i):=0]
  return(DT)
  
}

funciones_ordenar_fecha_semana<-function(DT,tipo="SEMANA")
{
  if(tipo=="SEMANA")
  {
    data.table::setorderv(DT,cols="SEMANA")
  }
  if(tipo=="FECHA")
  {
    data.table::setorderv(DT,cols="FECHA")
  }
  return(DT)
}
funciones_add_var_dep<-function(prob_dat)
{
  #prob_dat[, ("PASO_ANT") := shift(.SD, n=1, fill=NA, type="lead"), .SDcols="PASO"]
  prob_dat[, ("PASO_ANT") := shift(.SD, n=1, fill=NA, type="lag"), .SDcols="PASO"]
  data.table::setorderv(prob_dat,"SEMANA")
  prob_dat=prob_dat[!is.na(PASO)]
  prob_dat=prob_dat[!is.na(PASO_ANT)]
  prob_dat[,Y:=base::sign(PASO-PASO_ANT),by=seq_len(nrow(prob_dat))]
  #prob_dat[,Y:=base::sign(PASO_ANT-PASO),by=seq_len(nrow(prob_dat))]
  data.table::setnames(prob_dat, "PASO_ANT","PASO" )
  return(prob_dat)
}


funciones_naive_bayes<-function(trainData)
{
  trainData[,CLUSTER:=NULL]
  tune_control <- e1071::tune.control(random = FALSE, nrepeat = 1, repeat.aggregate= min,sampling = c("cross"), sampling.aggregate = mean,
                                     cross = 10, best.model = TRUE, performances = TRUE,error.fun = NULL)
  laplace = 0
  metodos<-list(naive=e1071::naiveBayes,svm=e1071::svm)#,knn=class::knn)
  ranges<-list(naive=list(laplace = c(0,0.1,0.5,1.0)),svm=list(gamma = 10^(-5:-1), cost = 10^(-3:3)),knn=list(k=1:10))
  kernel =list(naive=NULL,svm="radial" )
  
  tuneMod<-list()
  tuneEror<-c()
  for(i in names(metodos))
  {
    tuneMod[[i]]<-e1071::tune(method=metodos[[i]],train.x= Y~., data=trainData, kernel=kernel[[i]],predict.fun=predict,probability=TRUE,ranges = ranges[[i]], tunecontrol=tune_control)  
    tuneEror<-c(tuneEror,tuneMod[[i]]$best.performance)
  }
  j<-names(metodos)[(which.min(tuneEror)[1])]
  mod<-tuneMod[[j]]$best.model
  return(mod)
}


funciones_naive_bayes_dev<-function(trainData)
{
# definimos la metrica para evaluar los modelos
  trainData[,CLUSTER:=NULL]
f1 <- function(data, lev = NULL, model = NULL) {
  f1_val <- MLmetrics::F1_Score(y_pred = data$pred,
                                y_true = data$obs,
                                positive = lev[3])
  c(F1 = f1_val)
}

 train.control <- caret::trainControl(method = "repeatedcv",
                               number = 10,
                               repeats = 10,
                               classProbs = TRUE,
                               #sampling = "smote",
                               summaryFunction = f1,
                               search = "grid")

#Grid parameter model
tune.grid.RF <- expand.grid(.mtry = seq(from = 1, to = 10, by = 1))
tune.grid.nb <- expand.grid(fL=c(0.1,0.5,1.0), usekernel = TRUE, adjust=c(0.1,0.5,1.0))
tune.grid.svmlinear <- expand.grid(C=c(0.01, 0.1, 1, 10, 100, 1000))
tune.grid.svmRadial <- expand.grid(sigma=seq(0.1,1,by=0.1), C=c(0.01, 0.1, 1, 10, 100, 1000))
tune.grid.svmRadialCost <- expand.grid(C=c(0.01, 0.1, 1, 10, 100, 1000))
tune.grid.svmRadialSigma <- expand.grid(sigma=seq(0.1,1,by=0.1),C=c(0.01, 0.1, 1, 10, 100, 1000))

tune.grid<-list(rf=tune.grid.RF,
                nb=tune.grid.nb,
                svmLinear=tune.grid.svmlinear,
                svmRadial=tune.grid.svmRadial,
                svmRadialCost=tune.grid.svmRadialCost,
                svmRadialSigma=tune.grid.svmRadialSigma
                )
methods<-names(tune.grid)
mod<-list()
metric<-c()
for(i in methods)
{
  mod.train <- caret::train(Y~., 
                           data = trainData,
                           method = i,
                           tuneGrid = tune.grid[[i]],
                           metric = "F1",
                           trControl = train.control)
 
  
  mod[[i]]<-mod.train
  
  
}


# whichTwoPct <- tolerance(gbmFit3$results, metric = "ROC", 
                         # tol = 2, maximize = TRUE)  

pred<-data.table::setDT(caret::extractPrediction(models=mod, testX = trainData[,-c("Y"),with=FALSE]))
pred<-split(pred,by="model")
pred<-base::lapply(X=pred,FUN=f1)
pred_min<-which.max(unlist(pred))[1]
pred_min<-unlist(strsplit(names(pred_min),"[.]"))[1]
mod_final<-list()
mod_final[[pred_min]]<-mod[[pred_min]]
return(mod_final)

}




funciones_umbral_velocidad<-function(dat)
{
  dat=funciones_dinamica_contagios(dat,territorio="CODIGO_COMUNA") 
  return(dat)
}


funciones_umbral_velocidad_train<-function(dat)
{
  dat=funciones_dinamica_contagios_train(dat,territorio="CODIGO_COMUNA") 
  return(dat)
}
funciones_umbral_velocidad_pred<-function(dat,pred_used,nSemanas)
{
  
  data.table::setnames(pred_used,"fit","CASOS")
  pred_used[,FECHA:=as.character(seq(as.Date(max(dat$FECHA))+1,length.out=nrow(pred_used),by="days"))][,c('lower','upper','METODO','ME','RMSE','MAE','MPE','MAPE','MASE','ACF1'):=NULL]
  dat<-data.table::rbindlist(list(dat,pred_used),fill = TRUE)
  dat[ , (names(dat)) := lapply(.SD, zoo::na.locf), .SDcols = names(dat)]
  
  
  out<-funciones_dinamica_contagios(dat,territorio="CODIGO_COMUNA")
  
  data.table::setnames(out,c("Quantile.0.025(R)","Median(R)","Quantile.0.975(R)"),c("OPTIMISTA","NORMAL","PESIMISTA"))
  
  out[, SEMANA:= as.Date(unique(cut(as.Date(FECHA), "week"))),by=seq_len(nrow(out))]
  out=out[, .SD[c(.N)], by=c("SEMANA","CODIGO_COMUNA")]
  out[,c("OPTIMISTA","NORMAL","PESIMISTA"):=list(OPTIMISTA<1,NORMAL<1,PESIMISTA<1)]
  
  
  out_melt<-data.table::melt(out,id.vars=c("SEMANA","CODIGO_COMUNA"), measure.vars=c("NORMAL","OPTIMISTA","PESIMISTA"),
                             variable.name = "ESCENARIOS", value.name = "R_UMBRAL")
  
  
  
  out_melt_split<-split(out_melt,by="ESCENARIOS")
  out_melt_split<-base::lapply(out_melt_split,utils::tail,n=nSemanas)
  return(out_melt_split)
}


funciones_umbral_velocidad_pred_parallel<-function(X,nSemanas,PASO_COMUNAL)
{
  dat<-X$dat
  pred_used<-X$pred_used
  data.table::setnames(pred_used,"fit","CASOS")
  pred_used[,FECHA:=as.Date(seq(as.Date(max(dat$FECHA))+1,length.out=nrow(pred_used),by="days"))][,c('lower','upper','METODO','ME','RMSE','MAE','MPE','MAPE','MASE','ACF1'):=NULL]
  dat<-data.table::rbindlist(list(dat,pred_used),fill = TRUE)
  dat[ , (names(dat)) := lapply(.SD, zoo::na.locf), .SDcols = names(dat)]
  
  
  out<-funciones_dinamica_contagios(dat,territorio="CODIGO_COMUNA")
  
  data.table::setnames(out,c("Quantile.0.025(R)","Median(R)","Quantile.0.975(R)"),c("OPTIMISTA","NORMAL","PESIMISTA"))
  
  out[, SEMANA:= as.Date(unique(cut(as.Date(FECHA), "week"))),by=seq_len(nrow(out))]
  out=out[, .SD[c(.N)], by=c("SEMANA","CODIGO_COMUNA")]
  out[,c("OPTIMISTA","NORMAL","PESIMISTA"):=list(OPTIMISTA<1,NORMAL<1,PESIMISTA<1)]
  
  
  out_melt<-data.table::melt(out,id.vars=c("SEMANA","CODIGO_COMUNA"), measure.vars=c("NORMAL","OPTIMISTA","PESIMISTA"),
                             variable.name = "ESCENARIOS", value.name = "R_UMBRAL")
  
  out_melt<-PASO_COMUNAL[out_melt,on=c("SEMANA","CODIGO_COMUNA")]
  out_melt[ , (names(out_melt)) := lapply(.SD, zoo::na.locf), .SDcols = names(out_melt)]
  
  out_melt_split<-split(out_melt,by="ESCENARIOS")
  out_melt_split<-base::lapply(out_melt_split,utils::tail,n=nSemanas)
  return(out_melt_split)
}



funciones_fill_gap<-function(dat)
{
  marco<-data.table::data.table(FECHA=(seq(as.Date(min(dat$FECHA,na.rm=TRUE)),as.Date(max(dat$FECHA,na.rm=TRUE)),by="days")))
  
  dat<-dat[marco,on="FECHA"]
  dat[,CASOS:=as.integer(zoo::na.approx(CASOS))]
  dat[ , (names(dat)) := lapply(.SD, zoo::na.locf), .SDcols = names(dat)]
  dat[, ("CASOS_1") := shift(.SD, 1, 0, "lag"), .SDcols="CASOS"]
  
  dat[, CASOS:=CASOS-CASOS_1]
  dat[, CASOS:=ifelse(CASOS>=0,CASOS,0),by=seq_len(nrow(dat))]
  dat[,CASOS_1:=NULL]
  return(dat)
  
}

funciones_sample<-function(DT)
{
  Nm<-max(DT[Y!=0,.(.N),by="Y"]$N,na.rm = TRUE)
  if(!is.infinite(Nm))
  {
    DT=DT[,.SD[sample(.N, min(Nm,.N))],by = "Y"]
  }else
  {
    DT=DT[,.SD[sample(.N, min(2,.N))],by = "Y"]
  }
  return(DT)
}


funciones_prediccion_modelo<-function(mod,testData)
{
  if(any(class(mod)%in%c("svm")))
  {
    pred<-attr(predict(mod,testData ,probability = TRUE),"probabilities")
  }
  
  if(any(class(mod)%in%c("naiveBayes")))
  {
    pred<-predict(mod, testData,"raw")
  }
  return(pred)
  
}



funciones_prediccion_modelo_dev<-function(mod,testData)
{

  pred<-caret::extractProb(models = mod,testX =testData )
  return(pred)
  
}


  
