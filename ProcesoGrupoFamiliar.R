rm(list = ls(all=T))
source("Funciones.r")
## Carga e instalacion de paquetes ----
pkgs <- c("data.table", "tidyverse", "RODBC", "stringi", "lubridate", "tictoc", "readxl", "geosphere", "doParallel", "DT")
Loadpkg(pkgs)
library(stringi)
library(readxl)
library(tidyverse)

#tic("Cargue")

## Trucos ----

#``campos` con espacios` `` # Alt 96


str(BD_SegEmpAportes)
str(BD_MediosdePagosCM2019)

rm(list = ls(all=T))

tic("AfiliadosVirtual")
conecta_Afiliados2020 <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Persona/Afiliados 2020.accdb")
BD_AfiliadosMes <- sqlQuery(conecta_Afiliados2020 , paste0("select id_persona, codigo_segmento_poblacional, categoria, mes, grupo_familiar, codigo_tipo_aportante from Afiliados"), as.is=T)


rm(BD_AfiliadosMes, BD_Personas, BD_Virtual)
odbcCloseAll() 
toc()


base <-readRDS("D:/Compartido/MediosPago/2018BD_MediosdePagosCM2018.rds")
table(BD_MediosdePagosCM2018$Fecha_2)


#pattern = "*.xlsx"
#pattern = "*.csv"
#pattern = "*.rds"
#pattern = "*.txt"

ConexionCarpetaMP2017 <- "D:/Compartido/MediosPago/2017"
ContTemporal <- paste0(ConexionCarpetaMP2017,"/", list.files(ConexionCarpetaMP2017, pattern = "*.xlsx", recursive = F));ContTemporal

BD_MediosdePagosCM2017$BOLSILLO #permite ver las variables

table(BD_MediosdePagosCM2017$`FECHA TX`)

# Guardar

saveRDS(Consolidada, paste0("Consolidacion",mes,".rds"))
fwrite(Consolidada, "Consolidacion.csv")
fwrite(Consolidada, "Consolidacion.txt", sep = "\t")

## Empresas ----

rm(list = ls(all=T))

tic("VerEmpresas")

conecta_BDClientes <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Empresa/Empresa.accdb")
BD_Cliente <- sqlQuery(conecta_BDClientes , paste0("select * from Cliente"), as.is=T)



rm(BD_Cliente)
odbcCloseAll() 
toc()





## Base Empresas Simple  - Mensual ----
rm(list = ls(all=T))

Ar_Mes <- "Febrero2020"

tic("Empresas_Simple")
conecta_BD <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Empresa/Empresa.accdb")
Lis_Empresa <- sqlQuery(conecta_BD , paste0("select id_empresa, estado_empresa from Segmento"), as.is=T) %>% 
  filter(estado_empresa == "al día")
Lis_Clientes <- sqlQuery(conecta_BD , paste0("select * from Cliente"), as.is=T)

conecta_Base_Mes <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Base_Mes/Segmento_poblacional.accdb")
num_trabaj_empresa <- sqlQuery(conecta_Base_Mes, paste0("select id_empresa, id_persona from afiliado_con_empresa"), as.is=T) %>% 
  group_by(id_empresa) %>% 
  summarise(nume_trabajador=n())

conecta_TablaConver <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Tabla_Conversion/Tabla_conversion.accdb")
tipo_doc_empresa <- sqlQuery(conecta_TablaConver , paste0("select codigo_tipo_documento, siglas_tipo_documento from Tb_Tipo_Documento"), as.is=T)

Lis_Clientes <- Lis_Clientes %>% 
  left_join(tipo_doc_empresa, by = "codigo_tipo_documento")

BD_Simple <- Lis_Empresa %>% 
  left_join(Lis_Clientes, by = "id_empresa") %>% 
  left_join(num_trabaj_empresa, by = "id_empresa")

fwrite(BD_Simple %>% select(id_empresa, siglas_tipo_documento, numero_documento_txt, numero_documento_sin_digito, razon_social, nume_trabajador),paste0("//boga04beimrodc/REQUERIMIENTO/EntregasMes/BD_Simple",Ar_Mes,".csv"))

rm(Lis_Empresa, Lis_Clientes, num_trabaj_empresa, tipo_doc_empresa)
rm(list = ls(all=T))
odbcCloseAll() 
toc()

## Base Call Center Camilo Garzon ----

rm(list = ls(all=T))

Ar_Mes <- "Feb2020"
ValMes <- 2
ValAnio <- 2020


tic("AfiliadoCallCenter")
conecta_SegmEmpresas <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Empresa/Empresa.accdb")
conecta_SegmPoblacion <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Base_Mes/Segmento_poblacional.accdb")


BD_CallCenter1 <- sqlQuery(conecta_SegmPoblacion , paste0("select * from afiliado_con_empresa"), as.is=T)
BD_CallCenter2 <- sqlQuery(conecta_SegmPoblacion , paste0("select * from afiliado_mes_unico"), as.is=T)
BD_CallCenter3 <- sqlQuery(conecta_SegmEmpresas, paste0("select * from Segmento"), as.is=T)

BD_Call_Center <- BD_CallCenter1 %>% 
  left_join(BD_CallCenter2, by = "id_persona") %>%
  left_join(BD_CallCenter3, by = "id_empresa") %>% 
  select(id_empresa, id_persona, categoria, Segmento_poblacional, marca_afiliado_unico, total_numero_grupo_familiar, Salario.x, piramide_1, piramide_2) %>% 
  mutate(mes = ValMes, año = ValAnio)

fwrite(BD_Call_Center ,paste0("//boga04beimrodc/REQUERIMIENTO/EntregasMes/BD_CallCenter_Mes",Ar_Mes,".csv"))


rm(BD_CallCenter1, BD_CallCenter2, BD_CallCenter3,BD_Call_Center)
rm(list = ls(all=T))
odbcCloseAll() 
toc()




## Base de datos Virtual ----
rm(list = ls(all=T))
ValAnio <- 2020

tic("AfiliadosVirtual")
conecta_Afiliados2020 <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Persona/Afiliados 2020.accdb")
conecta_Personas <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Persona/PERSONA.accdb")
Conecta_Virtual <- odbcDriverConnect("Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=D:/REQUERIMIENTO/Ans_Contactabilidad/Virtual/AFILIADOS_VIRTUAL-2020.accdb")


BD_AfiliadosMes <- sqlQuery(conecta_Afiliados2020 , paste0("select id_persona, codigo_segmento_poblacional, categoria, mes, grupo_familiar, codigo_tipo_aportante from Afiliados"), as.is=T)

BD_Personas <- sqlQuery(conecta_Personas , paste0("select * from Persona"), as.is=T) %>% 
  filter(Id_persona %in% BD_AfiliadosMes$id_persona)



BD_Virtual <- BD_AfiliadosMes %>% 
  left_join(BD_Personas, by = c("id_persona" = "Id_persona")) %>% 
  select(id_persona, Id_tipo_documento, Tx_documento_persona, Primer_nombre, Segundo_nombre, Primer_apellido, Segundo_apellido, Edad, codigo_segmento_poblacional, categoria, mes, grupo_familiar, codigo_tipo_aportante) %>% 
  mutate(año = ValAnio)

names(BD_Virtual)

fwrite(BD_Virtual ,"//boga04beimrodc/REQUERIMIENTO/EntregasMes/BD_AfiliadosColsubsidio2020.csv")
#sqlUpdate(channel = Conecta_Virtual, dat =  BD_Virtual, tablename = BD_Virtual)


rm(BD_AfiliadosMes, BD_Personas, BD_Virtual)
odbcCloseAll() 
toc()

## Bases mes Salud ----

rm(list = ls(all=T))


tic("Bases Salud")



rm(BD_AfiliadosMes, BD_Personas, BD_Virtual)
odbcCloseAll() 
toc()








## MHE para Empresas Grandes ----

rm(list = ls(all=T))

SegMHEGruoEmp = "1 Emp Grandes"

tic("MHE-Grandes")
conecta_SegmEmpresas <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Empresa/Empresa.accdb")
conecta_SegmPoblacion <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Base_Mes/Segmento_poblacional.accdb")
conecta_Base_Mes <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Base_Mes/Base_mes.accdb")
conecta_TablaConver <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Tabla_Conversion/Tabla_conversion.accdb")
conecta_Personas <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Persona/PERSONA.accdb")
# Extraer información y unir

BD_MHE1 <- sqlQuery(conecta_SegmPoblacion , paste0("select * from afiliado_con_empresa"), as.is=T)
BD_MHE2 <- sqlQuery(conecta_SegmPoblacion , paste0("select * from afiliado_mes_unico"), as.is=T)
BD_MHE3 <- sqlQuery(conecta_Personas , paste0("select Id_persona, Genero, Edad from Persona"), as.is=T)
BD_MHE4 <- sqlQuery(conecta_Base_Mes, paste0("select  * from Base_mes"), as.is=T)
BD_MHE5 <- sqlQuery(conecta_TablaConver, paste0("select * form Tb_segmento_grupo_familia"))

cuota <- BD_MHE4 %>% fitler(numero_cuota_monetaria >=1)


BD_MHE_total <- BD_MHE1 %>% 
  left_join(BD_MHE2, by = "id_persona")

BD_MHE_total <- BD_MHE_total %>% 
  left_join(BD_MHE3, by = c("id_persona" = "Id_persona")) %>% 
  mutate(rangoEdad=case_when(
    Edad <=19 ~ "Menor a 19",
    Edad >=20 & Edad<=35 ~ '20 a 35 años',
    Edad >=36 & Edad<=45 ~ '36 a 45 años',
    Edad >=46 & Edad<=55 ~ '47 a 55 años',
    Edad >=55 ~ 'Mayor de 55 años'
    T ~ "")) %>%
  tate(Salario1=case_when(
    Salario < 1843386 ~ "Menor 2.1 SLMMV",
    Salario >= 1843386 & Salario <= 3511212 ~ "2.1 a 4 SLMMV",
    Salario >= 3598992 & Salario <= 7022424 ~ "4.1 a 8 SLMMV",
    Salario >= 7110204 & Salario <= 17468279 ~ "8.1 a 19.9 SLMMV",
    Salario > 17468279 ~ "Mayores 19.9 SLMMV"
    T ~ "")) %>% 
  BD_MHE_total <- BD_MHE_total %>% 
  left_join(BD_MHE4, by = "id_persona") %>% 
  
  BD_MHE_total <- BD_MHE_total %>% 
  left_join(BD_MHE5, by = "codigo_segmento_grupo_familiar")%>%
  
  
  
  
  
  
  
  
  
  
  rm(BD_AfiliadosMes, BD_Personas, BD_Virtual)
odbcCloseAll() 
toc()



## Nombre de Niños, Nombre de Niñas y Apellidos ----

rm(list = ls(all=T))
tic("AjusteNombres")
conecta_Personas <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Persona/PERSONA.accdb")
BD_Nombre1 <- sqlQuery(conecta_Personas , paste0("select Primer_nombre, Genero from Persona"), as.is=T) %>% 
  select (Primer_nombre) %>%  unique()
BD_Nombre2 <- sqlQuery(conecta_Personas , paste0("select Segundo_nombre, Genero from Persona"), as.is=T) %>% 
  select (Segundo_nombre) %>%  unique()
BD_Nombres <- bind_rows(BD_Nombre1, BD_Nombre2)

BD_Apellido1 <- sqlQuery(conecta_Personas , paste0("select Primer_apellidox from Persona"), as.is=T) %>% 
  select (Primer_apellido) %>%  unique()
BD_Apellido2 <- sqlQuery(conecta_Personas , paste0("select Segundo_apellido from Persona"), as.is=T) %>% 
  select (Segundo_apellido) %>%  unique()
BD_Apellidos <- bind_rows(BD_Apellido1, BD_Apellido2)

fwrite(BD_Nombres ,"BD_NombresGenero.csv")
fwrite(BD_Apellidos ,"BD_Apillidos.csv")

rm(BD_Nombre1, BD_Nombre2, BD_Nombres, BD_Apellido1, BD_Apellido2, BD_Apellidos)
odbcCloseAll() 
toc()

## Informe 1  Aventureros ----

rm(list = ls(all=T))

tic("InformeAventureros")

conecta_Aventureros <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Filial/Aventurero.accdb")
conecta_AventurerosBase <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Filial/Aventurero_Base.accdb")
conecta_SegmEmpresas <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Empresa/Empresa.accdb")
conecta_SegmPoblacion <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Base_Mes/Segmento_poblacional.accdb")
conecta_Personas <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Persona/PERSONA.accdb")
conecta_GrupoFamiliar <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Persona/Grupo_familiar.accdb")
conecta_Autorizacion <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Contacto/Fuentes/Autorizacion.accdb")
conecta_Celular <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Contacto/Fuentes/Celular.accdb")
conecta_Mail <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=//bogak08beimrodc/BI/Contacto/Fuentes/Mail.accdb")


BD_AventurerosBase <- sqlQuery(conecta_AventurerosBase, paste0("select * from Afiliado_aventurero"), as.is=T)
BD_Aventureros <- sqlQuery(conecta_Aventureros, paste0("select * from Aventurero"), as.is=T)
BD_AfiliadoEmpresa <- sqlQuery(conecta_SegmPoblacion, paste0("select * from afiliado_con_empresa"), as.is=T) %>% 
  filter(id_persona %in% BD_Aventureros$id_persona)

BD_AfilUnico <- sqlQuery(conecta_SegmPoblacion, paste0("select * from afiliado_mes_unico"), as.is=T)
BD_Personas <- sqlQuery(conecta_Personas, paste0("select * from Persona"), as.is=T)
BD_Segmento <- sqlQuery(conecta_SegmEmpresas, paste0("select * from Segmento"), as.is=T)
BD_Autorizacion <- sqlQuery(conecta_SegmEmpresas, paste0("select * from tb_autorizaciones"), as.is=T)

BD_InformeParcialAventureros <- BD_AventurerosBase %>%
  inner_join(BD_Aventureros, by = "id_aventurero") %>%
  filter(id_persona)

names(BD_AventurerosBase)

BD_InformeAventureros <- BD_AfiliadoEmpresa %>% 
  left_join(BD_Aventureros, by = "id_persona") %>% 
  left_join(BD_AfilUnico, by = "id_persona") %>% 
  left_join(BD_Segmento, by = "id_empresa") %>%
  left_join(BD_Autorizacion, by = "id_persona")

BD_InformeFinal <- BD_InformeAventureros %>%
  left_join(BD_InformeAventureros, by = "id_aventurero")




rm(BD_AventurerosBase, BD_Aventureros, BD_AfiliadoEmpresa, BD_AfilUnico, BD_Personas, BD_Segmento, BD_Autorizacion, BD_InformeParcialAventureros, BD_InformeAventureros)
odbcCloseAll() 
toc()




## Consolidado Medios de Pago Cuota Monetaria ----

rm(list = ls(all=T))

tic("BasesMediosdePago")

ConexionCarpetaMP2017 <- "D:/Compartido/MediosPago/2017"
ContTemporal <- paste0(ConexionCarpetaMP2017,"/", list.files(ConexionCarpetaMP2017, pattern = "*.xlsx", recursive = F))

nom_archivo <- gsub(pattern = '.xlsx',replacement = '',list.files(ConexionCarpetaMP2017, pattern = "*.xlsx", recursive = F))

BD_MediosdePagosCM2017 <- NULL

for (i in 1:12){
  tmp <- read_excel(ContTemporal[i],sheet = "Detallado") %>% 
    mutate(
      TARJETA = as.character(TARJETA), 
      FECHA_2 = as.Date(FECHA_2,"%dd/%mm/%Y"), 
      `ID TERMINAL`= as.numeric(`ID TERMINAL`), 
      Nombre_Archivo = as.character(nom_archivo[i])
      )
  
  BD_MediosdePagosCM2017 <- bind_rows(BD_MediosdePagosCM2017, tmp)
  rm(tmp)
  print(paste('ok base'),nom_archivo[i])
  
}

saveRDS(BD_MediosdePagosCM2017, paste0(ConexionCarpetaMP2017,"BD_MediosdePagosCM2017.rds"))
fwrite(BD_MediosdePagosCM2017 ,paste0(ConexionCarpetaMP2017,"BD_MediosdePagosCM2017.csv"))

rm(ContTemporal, nom_archivo, BD_MediosdePagosCM2017, i)
toc()

# archivo de prueba
prueba <-BD_MediosdePagosCM2017  %>% data.frame()

# pasara nombre de variables a mayusc
names(prueba) <- gsub(pattern = '.', replacement = '_',  toupper(names(prueba)),fixed = T)

# convertir a data frame (condiciones)
prueba <-prueba %>% data.frame()

# pasara nombre de variables a mayusc
names(prueba) <- gsub(pattern = '.', replacement = '_',  toupper(names(prueba)),fixed = T)

# verificar nombre de las variables
table(duplicated(toupper(names(prueba))))

prueba <- prueba %>% select(BOLSILLO, COD_COMPENSACION,TARJETA)

str(prueba)
# abrir conexiones
conecta_MdPCM <- odbcDriverConnect( "Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=D:/Compartido/MediosPago/MediosPago2017.accdb")


sqlSave(channel = conecta_MdPCM,dat = prueba ,tablename = 'MPCM2017',rownames = F,append = T)

odbcCloseAll()


## medios de pago 2018----
rm(list = ls(all=T))

tic("BasesMediosdePago2018")

ConexionCarpetaMP2018 <- "D:/Compartido/MediosPago/2018"
ContTemporal <- paste0(ConexionCarpetaMP2018,"/", list.files(ConexionCarpetaMP2018, pattern = "*.xlsx", recursive = F))

nom_archivo <- gsub(pattern = '.xlsx',replacement = '',list.files(ConexionCarpetaMP2018, pattern = "*.xlsx", recursive = F))

BD_MediosdePagosCM2018 <- NULL

for (i in 1:12){
  tmp <- read_excel(ContTemporal[i],sheet = "Detallado", col_types = 'text') %>% 
    mutate(
      TARJETA = as.character(TARJETA), 
      FechaTransaccion= as.Date(as.numeric(FECHA_2), origin="1899-12-30") ,
      CodigoEstablecimiento = as.character(`COD ESTABLECIMIENTO`), 
      ValorTransaccion = as.numeric(`VR TX`),
      TipoTransaccion = as.character(`TIPO TX`),
      Nombre_Archivo = as.character(nom_archivo[i])
    )
  
  BD_MediosdePagosCM2018 <- bind_rows(BD_MediosdePagosCM2018, tmp)
  rm(tmp)
  #print(paste('ok base'),nom_archivo[i])
  
}

BDCargMP2018 <- BD_MediosdePagosCM2018 %>% 
  select(TARJETA, FechaTransaccion, CodigoEstablecimiento, ValorTransaccion, TipoTransaccion)

saveRDS(BD_MediosdePagosCM2018, paste0(ConexionCarpetaMP2018,"BD_MediosdePagosCM2018.rds"))
fwrite(BD_MediosdePagosCM2018 ,paste0(ConexionCarpetaMP2018,"BD_MediosdePagosCM2018.csv"))

saveRDS(BDCargMP2018, "D:/Compartido/MediosPago/BD_MediosdePagosCM2018.rds")
#MPTem <- bind_rows(BDCargMP2018, readRDS ("D:/Compartido/MediosPago/BaseCargueMediosdePago.rds"))


rm(ContTemporal, nom_archivo, BD_MediosdePagosCM2018, i, BDCargMP2018)

odbcCloseAll()
rm(list = ls(all=T))
toc()

## medios de pago 2017 ----

rm(list = ls(all=T))

tic("BasesMediosdePago2017")

ConexionCarpetaMP2017 <- "D:/Compartido/MediosPago/2017"
ContTemporal <- paste0(ConexionCarpetaMP2017,"/", list.files(ConexionCarpetaMP2017, pattern = "*.xlsx", recursive = F))

nom_archivo <- gsub(pattern = '.xlsx',replacement = '',list.files(ConexionCarpetaMP2017, pattern = "*.xlsx", recursive = F))

BD_MediosdePagosCM2017 <- NULL

for (i in 1:12){
  tmp <- read_excel(ContTemporal[i],sheet = "Detallado", col_types = 'text') %>% 
    mutate(
      TARJETA = as.character(TARJETA), 
      FechaTransaccion= as.Date(as.numeric(FECHA_2), origin="1899-12-30") ,
      CodigoEstablecimiento = as.character(`COD ESTABLECIMIENTO`), 
      ValorTransaccion = as.numeric(`VR TX`),
      TipoTransaccion = as.character(`TIPO TX`),
      Nombre_Archivo = as.character(nom_archivo[i])
    )
  
  BD_MediosdePagosCM2017 <- bind_rows(BD_MediosdePagosCM2017, tmp)
  rm(tmp)
  #print(paste('ok base'),nom_archivo[i])
  
}

BDCargMP2017 <- BD_MediosdePagosCM2017 %>% 
  select(TARJETA, FechaTransaccion, CodigoEstablecimiento, ValorTransaccion, TipoTransaccion)


saveRDS(BD_MediosdePagosCM2017, paste0(ConexionCarpetaMP2017,"BD_MediosdePagosCM2017.rds"))
fwrite(BD_MediosdePagosCM2017 ,paste0(ConexionCarpetaMP2017,"BD_MediosdePagosCM2017.csv"))

saveRDS(BDCargMP2017, "D:/Compartido/MediosPago/BD_MediosdePagosCM2017.rds")

rm(ContTemporal, nom_archivo, BD_MediosdePagosCM2018, i, BDCargMP2017)

odbcCloseAll()
rm(list = ls(all=T))
toc()

## Medios de Pago 2019 ----


rm(list = ls(all=T))

tic("BasesMediosdePago2017")

ConexionCarpetaMP2019 <- "D:/Compartido/MediosPago/2019"
ContTemporal <- paste0(ConexionCarpetaMP2019,"/", list.files(ConexionCarpetaMP2019, pattern = "*.xlsx", recursive = F))

nom_archivo <- gsub(pattern = '.xlsx',replacement = '',list.files(ConexionCarpetaMP2019, pattern = "*.xlsx", recursive = F))

BD_MediosdePagosCM2019 <- NULL

for (i in 1:12){
  tmp <- read_excel(ContTemporal[i],sheet = "Detallado", col_types = 'text') %>% 
    mutate(
      TARJETA = as.character(TARJETA), 
      FechaTransaccion= as.Date(as.numeric(FECHA_2), origin="1899-12-30") ,
      CodigoEstablecimiento = as.character(`COD ESTABLECIMIENTO`), 
      ValorTransaccion = as.numeric(`VR TX`),
      TipoTransaccion = as.character(`TIPO TX`),
      Nombre_Archivo = as.character(nom_archivo[i])
    )
  BD_MediosdePagosCM2019 <- bind_rows(BD_MediosdePagosCM2019, tmp)
  rm(tmp)
  #print(paste('ok base'),nom_archivo[i])
  }

BDCargMP2019 <- BD_MediosdePagosCM2019 %>% 
  select(TARJETA, FechaTransaccion, CodigoEstablecimiento, ValorTransaccion, TipoTransaccion)


saveRDS(BD_MediosdePagosCM2019, paste0(ConexionCarpetaMP2019,"BD_MediosdePagosCM2019.rds"))
fwrite(BD_MediosdePagosCM2019 ,paste0(ConexionCarpetaMP2019,"BD_MediosdePagosCM2019.csv"))

saveRDS(BDCargMP2019, "D:/Compartido/MediosPago/BD_MediosdePagosCM2019.rds")


rm(ContTemporal, nom_archivo, BD_MediosdePagosCM2019, i, BDCargMP2019)

odbcCloseAll()
rm(list = ls(all=T))
toc()


## Medios de Pago 2020


#ContTemporal <- paste0(ConexionCarpetaMP2020,"/", list.files(ConexionCarpetaMP2020, pattern = "*.xlsx", recursive = F));ContTemporal

rm(list = ls(all=T))

tic("BasesMediosdePago2020")

ConexionCarpetaMP2020 <- "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/2020/TMP"
ContTemporal <- paste0(ConexionCarpetaMP2020,"/", list.files(ConexionCarpetaMP2020, pattern = "*.xlsx", recursive = F)) # Concatenar los nombres de los
# archivos a la ruta del directorio padre y por cada ruta, se guarda como un string en un arreglo

nom_archivo <- gsub(pattern = '.xlsx',replacement = '',list.files(ConexionCarpetaMP2020, pattern = "*.xlsx", recursive = F)) # Obtener un vector con los
# nombres de los archivos sin extensión

BD_MediosdePagosCM2020 <- NULL

# Para ejecutar las instrucciones del ciclo, deben estar cerrados los archivos
for (i in 1:3){ # Actualizar al número de archivos en la carpeta TMP
  tmp <- read_excel(ContTemporal[i],sheet = "AUMV", col_types = 'text', range = cell_cols("A:U")) %>% 
    mutate(
      TARJETA = as.character(`NUMERO DE TARJETA`), 
      FechaTransaccion= as.Date(as.numeric(`FECHA TRANSACCION`), origin="1899-12-30"),
      CodigoEstablecimiento = as.character(`CODIGO ESTABLECIMIENTO`), 
      ValorTransaccion = as.numeric(`VALOR TRANSACCION`),
      TipoTransaccion = as.character(`TIPO DE TRANSACCION`),
      Nombre_Archivo = as.character(nom_archivo[i])
    )
  
  BD_MediosdePagosCM2020 <- bind_rows(BD_MediosdePagosCM2020, tmp)
  rm(tmp)
  #print(paste('ok base'),nom_archivo[i])
  
}

# Genera los archivo diario suministrados en la carpeta TMP (Actualizar histórico) -----
BD_Temporal <- readRDS("D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2020.rds")
BD_Temporal_2 <- readRDS("D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/2020BD_MediosdePagosCM2020.rds")

BDCargMP2020 <- BD_MediosdePagosCM2020 %>% 
  select(TARJETA, FechaTransaccion, CodigoEstablecimiento, ValorTransaccion, TipoTransaccion)

BD_CreTem <- bind_rows(BD_Temporal, BDCargMP2020) # Anexar los nuevos registros al dataset
saveRDS(BD_CreTem, "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2020.rds") # Sobreescribir
#  los datos del archivo "BD_MediosdePagosCM2020.rds"

BD_CreTem <- BD_CreTem %>% 
  mutate(mes= as.numeric(format(FechaTransaccion, '%m')),
         Valor = format(ValorTransaccion, scientific = FALSE))

fwrite(BD_CreTem, "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2020.csv") # Sobreescribir
#  los datos del archivo "BD_MediosdePagosCM2020.csv"

BD_CreTem_2 <- bind_rows(BD_Temporal_2, BD_MediosdePagosCM2020)

saveRDS(BD_CreTem_2, "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/2020BD_MediosdePagosCM2020.rds") # Sobreescribir
#  archivo "2020BD_MediosdePagosCM2020.rds"
fwrite(BD_CreTem_2, "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/2020BD_MediosdePagosCM2020.csv") # Crear
# archivo csv

RV2020 <- BD_CreTem %>% 
  group_by(FechaTransaccion = format(FechaTransaccion, '%Y%B')) %>% 
  summarise(ValorTransaccion = sum(ValorTransaccion),
            freq =  n()) # Generar estadísticas
view(RV2020)

Nuevos2020 <- BD_MediosdePagosCM2020 %>% 
  group_by(FechaTransaccion = format(FechaTransaccion, '%Y%B')) %>% 
  summarise(ValorTransaccion = sum(ValorTransaccion),
            freq =  n()) # Generar estadísticas
view(Nuevos2020)

# Fin Actualizar histórico --------

# para crear la base y los archivos (Solo registros nuevos) -------
BDCargMP2020 <- BD_MediosdePagosCM2020 %>% 
  select(TARJETA, FechaTransaccion, CodigoEstablecimiento, ValorTransaccion, TipoTransaccion)

saveRDS(BD_MediosdePagosCM2020, "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/2020BD_MediosdePagosCM2020.rds")
fwrite(BD_MediosdePagosCM2020 , "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/2020BD_MediosdePagosCM2020.csv")

saveRDS(BDCargMP2020, "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2020.rds")


##Generando csv para los años

BD_Tem2017 <- readRDS("D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2017.rds")
fwrite(BD_Tem2017 , "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2017.csv")

RV2020 <-BD_Tem2017 %>% 
  group_by(FechaTransaccion = format(FechaTransaccion, '%Y%B')) %>% 
  summarise(ValorTransaccion = sum(ValorTransaccion),
            freq =  n())

BD_Tem2018 <- readRDS("D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2018.rds")
fwrite(BD_Tem2018 , "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2018.csv")

BD_Tem2019 <- readRDS("D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2019.rds") %>% 
  mutate(mes= as.numeric(format(FechaTransaccion, '%m')), 
           ValorTrans = as.character(ValorTransaccion))

fwrite(BD_Tem2019 , "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2019.csv")

BD_Tem2020 <- readRDS("D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2020.rds") %>% 
  mutate(mes= as.numeric(format(FechaTransaccion, '%m')),
         Valor = format(ValorTransaccion, scientific = FALSE))
fwrite(BD_Tem2020 , "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2020.csv")
# Fin Solo registros nuevos -------------------

# Crear base para anexar registros nuevos de la carpeta TMP ---------------------
saveRDS(BDCargMP2020, "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2020Anexar.rds")

RV2020 <-BDCargMP2020 %>% 
  group_by(FechaTransaccion = format(FechaTransaccion, '%Y%B')) %>% 
  summarise(ValorTransaccion = sum(ValorTransaccion),
            freq =  n())

rm(ContTemporal, nom_archivo, BD_MediosdePagosCM2020, i)

BD_TemAn2020 <- readRDS("D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2020Anexar.rds") %>% 
  mutate(mes= as.numeric(format(FechaTransaccion, '%m')),
         Valor = format(ValorTransaccion, scientific = FALSE))
fwrite(BD_TemAn2020, "D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2020Anexar.csv")
# Fin anexar registros nuevos de la carpeta TMP --------------------

#Verifica1 <- BD_Tem2020 %>% 
#  filter(TARJETA == "8800010063094373")


c <-BD_TemAn2020 %>% 
  group_by(FechaTransaccion = format(FechaTransaccion, '%Y%B')) %>% 
  summarise(ValorTransaccion = sum(ValorTransaccion),
            freq =  n())
c

names(BD_TemAn2020)

odbcCloseAll() # Cerrar conexión a la base de datos
rm(list = ls(all=T))
toc()


## Fin


VerArchivo <- readRDS("D:/Compartido/MediosPago/BD_MediosdePagosCM2018.rds")
names(VerArchivo)

c <-VerArchivo %>% 
  group_by(FechaTransaccion = format(FechaTransaccion, '%Y%B')) %>% 
  summarise(ValorTransaccion = sum(ValorTransaccion),
            freq =  n())
c

a <- VerArchivo %>% 
  distinct(TipoTransaccion)
a


V2020 <-readRDS("D:/ProcesosProteccionSocial/ProcesosCuotaMonetaria/MediosPago/BD_MediosdePagosCM2020.rds")

str(V2020)
glimpse(V2020) # Visualizar observaciones y variables
RV2020 <-V2020 %>% 
  group_by(FechaTransaccion = format(FechaTransaccion, '%Y%B')) %>% 
  summarise(ValorTransaccion = sum(ValorTransaccion),
            freq =  n())

odbcCloseAll() # Cerrar conexión a la base de datos
rm(list = ls(all=T))
toc()

