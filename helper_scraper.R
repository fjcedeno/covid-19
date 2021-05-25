# General-purpose data wrangling
library(tidyverse)  
# Parsing of HTML/XML files  
library(rvest)    
# String manipulation
library(stringr)   
# Verbose regular expressions
####################################################
#X[grepl("^m.*\\.log", X)]
scraper_files_product<-function(html)
{
  product<-html %>%read_html() %>% html_nodes(".markdown-body") %>% html_nodes("a")%>% html_attr("href")
  product<-product[grepl("/output/", product)] 
  product<-unlist(strsplit(product,"/"))
  product<-product[grepl("^producto*", product)]
  return(product)
}

scraper_files_csv<-function(html)
{
  files<-html %>%read_html() %>% html_nodes(".js-navigation-open") %>% html_attr("title") %>% na.omit() 
  files<-files[grepl("*\\.csv", files)] 
  return(files)
}


scraper_files_desc<-function(html)
{

  
  
descripcion<-html %>%read_html() %>% html_nodes(".markdown-body") %>% html_nodes("a") %>% html_text("a")
descripcion<-descripcion[grepl("Data", descripcion)] 
return(descripcion)
}

scraper_wiki_table<-function()
{
  html<-"https://es.wikipedia.org/wiki/Anexo:Comunas_de_Chile"
  out=html %>%read_html() %>% html_nodes('body #content #bodyContent #mw-content-text .mw-parser-output') %>% html_nodes('table') %>%
    html_table() %>% data.table::rbindlist()
  colnames(out)=paste(names(out),1:ncol(out))
  data.table::setnames(out,old=names(out),new=c("codigo_comuna","comuna_residencia","V1","provincia_residencia",
                                            "region_residencia","superficie_km_2","poblacion","densidad","idh_numero","idh_desc","lat","lon"),skip_absent = TRUE)
  out[,V1:=NULL]
  
  out[,superficie_km_2:=paste0(unlist(strsplit(superficie_km_2,split='\\.')),collapse = ""),by=seq_len(nrow(out))]
  out[,superficie_km_2:=gsub('\\,', "\\.",superficie_km_2),by=seq_len(nrow(out))]
  out[,idh_numero:=gsub('\\,', "\\.",idh_numero),by=seq_len(nrow(out))]
  fkt=c("superficie_km_2","idh_numero")
  out[ , (fkt) := lapply(.SD, raster::trim), .SDcols = fkt]
  out[ , (fkt) := lapply(.SD, as.numeric), .SDcols = fkt]
  out=out[,c("codigo_comuna","superficie_km_2","idh_numero","idh_desc"),with=FALSE]
  return(out)
}

scraper_emol_table<-function()
{
  html<-"https://www.emol.com/especiales/2020/internacional/coronavirus/guia-basica.asp"
  out=html %>%read_html() %>% html_nodes('body #inner-wrap #nota_tabla_emol') %>%  html_nodes('table') %>% html_table() %>% data.table::rbindlist()
  data.table::setnames(out,old=names(out),new=c("COMUNA_RESIDENCIA_DESC","PASO"),skip_absent = TRUE)
  MARCO_COMUNAL<-hana_get_complete_table(jdbcConnection=hanaConnection,nombre="COVID_DATOS_COMUNAS")
  
  out[,COMUNA_RESIDENCIA:=unlist(strsplit(COMUNA_RESIDENCIA_DESC,"Desde"))[1],seq_len(nrow(out))]
  out[,Paso_prob:=unlist(strsplit(COMUNA_RESIDENCIA_DESC,"Desde"))[2],seq_len(nrow(out))]
  
  out[,COMUNA_RESIDENCIA:=gsub(">","",COMUNA_RESIDENCIA),by=seq_len(nrow(out))]
  out[,COMUNA_RESIDENCIA:=raster::trim(COMUNA_RESIDENCIA),by=seq_len(nrow(out))]
  out[,Paso_prob:=raster::trim(Paso_prob),by=seq_len(nrow(out))]
  
  out[,Paso_prob:=tail(unlist(strsplit(Paso_prob," ")),1),seq_len(nrow(out))]
  out[,Paso_prob:=raster::trim(Paso_prob),by=seq_len(nrow(out))]
  emolEtapas<-openxlsx::read.xlsx("~/PROYECTO_ALLOCATION/xlsx/emolEtapas.xlsx")
  for(i in 1:nrow(emolEtapas))
  {
    out[Paso_prob==emolEtapas$ETAPAS_DESC[i],PASO:=emolEtapas$ETAPAS[i]]
  }
  emolComuna<-openxlsx::read.xlsx("~/PROYECTO_ALLOCATION/xlsx/emolComuna.xlsx")
  for(j in 1:nrow(emolComuna))
  { 
    out[COMUNA_RESIDENCIA== emolComuna$emol_comuna[j],COMUNA_RESIDENCIA:=emolComuna$comuna[j]]
  }
  out<-MARCO_COMUNAL[out,on="COMUNA_RESIDENCIA"]
  out<-out[,c("CODIGO_COMUNA","COMUNA_RESIDENCIA","PASO","Paso_prob"),with=FALSE]
  out[,Paso_prob:=!is.na(Paso_prob)]
  return(out)
}



