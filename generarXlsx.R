emol_comuna<-c("Til Til","Paihuano","Marchigüe","Ránquil","Los Álamos","Quilac","Aysén","Coyhaique","Llay-Llay","Los Ángeles","Zapalla","Catem","Papud")
comuna<-c("Tiltil","Paiguano","Marchihue","Ranquil","Los Alamos","Quilaco","Aisén","Coihaique","Llaillay","Los Angeles","Zapallar","Catemu","Papudo")

openxlsx::write.xlsx(data.table::data.table(emol_comuna=emol_comuna,comuna=comuna),file="~/PROYECTO_ALLOCATION/xlsx/emolComuna.xlsx")


ETAPAS_DESC<-c("Transición","Preparación","Inicial","Cuarentena")
ETAPAS<-c(2,3,4,1)

openxlsx::write.xlsx(data.table::data.table(ETAPAS_DESC=ETAPAS_DESC,ETAPAS=ETAPAS),file="~/PROYECTO_ALLOCATION/xlsx/emolEtapas.xlsx")





minCienciaRegiones<-c('Antofagasta','AraucanÃa','Araucania','Araucanía','Arica  y  Parinacota','Arica y Paricota','Arica y Parinacota','Atacama',
                      'AysÃ©n','Aysen','Aysén','BiobÃo','Biobio','Biobío','Coquimbo','Los  Lagos','Los Lagos','Los  Rios','Los RÃos','Los Rios','Los Ríos','Magallanes','Maule','Metropolita','Metropolitana','Ã‘uble','Nuble','Ñuble',"O'Higgins",'O’Higgins','Oâ€™Higgins','Tarapaca','Tarapacá','TarapacÃ¡','ValparaÃso','Valparaiso','Valparaíso')
Region<-c("Antofagasta","La Araucanía","La Araucanía","La Araucanía","Arica y Parinacota","Arica y Parinacota","Arica y Parinacota","Atacama",
          "Aysén del General Carlos Ibáñez del Campo","Aysén del General Carlos Ibáñez del Campo","Aysén del General Carlos Ibáñez del Campo" 
          ,"Biobío","Biobío","Biobío",'Coquimbo','Los Lagos','Los Lagos', "Los Ríos","Los Ríos","Los Ríos","Los Ríos","Magallanes y de la Antártica Chilena",'Maule',"Metropolitana de Santiago","Metropolitana de Santiago","Ñuble","Ñuble","Ñuble","Libertador General Bernardo O'Higgins","Libertador General Bernardo O'Higgins","Libertador General Bernardo O'Higgins",
          "Tarapacá","Tarapacá","Tarapacá","Valparaíso","Valparaíso","Valparaíso")


openxlsx::write.xlsx(data.table::data.table(minCienciaRegiones=minCienciaRegiones,Region=Region),file="~/PROYECTO_ALLOCATION/xlsx/minSalRegiones.xlsx")
