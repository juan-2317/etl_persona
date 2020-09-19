Loadpkg <- function(pkg){
  new.pkg <- pkg[!(pkg %in% installed.packages()[, "Package"])]
  if (length(new.pkg)) 
    install.packages(new.pkg, dependencies = T)
  sapply(pkg, require, character.only = TRUE)
}

convertir.contabilidad <- function(x){
  if(grepl("\\(.*\\)", x)){
    as.numeric(paste0("-", gsub("\\(|\\)", "", gsub("[\\$, ]", "", x))))
  } else {
    as.numeric(gsub("[\\$, ]", "", x))
  }
}

Limpiar.Cadenas <-function(x, espacios=T){
  require("stringi")
  x<-gsub("\\.", "_",tolower(gsub("\\W","",x)))
  x<-gsub("([\\W])\\1+","\\1",stri_trans_general(x,id="Latin-ASCII"), perl=T)
  if(!espacios){
    x<-gsub("\\s","",iconv(x,to="ASCII//TRANSLIT"), perl=T)
  } else {
        x
    }
}

Unir.Cadenas <- function(..., sep = " ", collapse = NULL, na.rm = F) {
    if (na.rm == F)
        paste(..., sep = sep, collapse = collapse)
    else
        if (na.rm == T) {
            paste.na <- function(x, sep) {
                x <- gsub("^\\s+|\\s+$", "", x)
                ret <- paste(na.omit(x), collapse = sep)
                is.na(ret) <- ret == ""
                return(ret)
            }
            df <- data.frame(..., stringsAsFactors = F)
            ret <- apply(df, 1, FUN = function(x) paste.na(x, sep))
            
            if (is.null(collapse))
                ret
            else {
                paste.na(ret, sep = collapse)
            }
        }
}

Cargar.Datos<-function (carpeta, extension="csv", exhaustivo=F, Fuente=T, n_ultimos=0, ordenado=T, ausentes=getOption("datatable.na.strings","NA"), 
                        separador="auto", dec=".", quote="\"", header="auto", clases=NULL){
    
    require("dplyr"); require("data.table"); require(qdapRegex)
    
    if(ordenado) file.list <- paste0(carpeta,"/",list.files(carpeta, pattern = paste0("+.",extension), recursive = exhaustivo))
    else file.list <- sort(paste0(carpeta,"/",list.files(carpeta, pattern = paste0("+.",extension), recursive = exhaustivo)))
    m=length(file.list)
    n=ifelse((n_ultimos>=m| n_ultimos<1),1, m-(n_ultimos-1))
    
    file.list<-file.list[n:length(file.list)]
    
    print("Importando:: ")
    print(file.list)
    Union <- do.call("bind_rows",
                     lapply(file.list, FUN = function(file) {
                         fread(file, sep=separador, dec=dec, quote=quote, header = header,
                               na.strings = ausentes,
                               col.names = Limpiar.Cadenas(names(fread(file.list[1], nrows = 1))),
                               colClasses = clases
                         ) %>% mutate(Source=as.character(rm_between(gsub(carpeta, "", file), "/", ".", extract=TRUE)[[1]]))
                       }
                     )
    )
    if(!Fuente)
      Union<-Union %>% select(-Source)
    return(Union)
}

Calcular.Edad = function(from, to) {
  from_lt = as.POSIXlt(from)
  to_lt = as.POSIXlt(to)
  
  age = to_lt$year - from_lt$year
  
  ifelse(to_lt$mon < from_lt$mon |
           (to_lt$mon == from_lt$mon & to_lt$mday < from_lt$mday),
         age - 1, age)
}

Calcuar.Meses <- function(from, to) {
  sd <- as.POSIXlt(from)
  ed <- as.POSIXlt(to)
  12 * (ed$year - sd$year) + (ed$mon - sd$mon)
}

Convertir.Fecha <- function(x, origen='1899-12-30'){
  options(warn=-1)
  x_trans <- as.Date(ifelse(substr(x,5,5)=="/",  as.character(as.Date(x, "%Y/%m/%d")),
                            ifelse(substr(x,5,5)=="-",  as.character(as.Date(x, "%Y-%m-%d")),
                                   ifelse(substr(x,3,3)=="/",  as.character(as.Date(x, "%d/%m/%Y")),
                                          ifelse(!is.na(as.numeric(x)), as.character(as.Date(as.numeric(x), origin = origen)),
                                                 as.Date(NA))))))
  options(warn=0)
}

Mode <- function(x, na.rm = FALSE) {
  if(na.rm){
    x = x[!is.na(x)]
  }
  ux <- unique(x)
  return(ux[which.max(tabulate(match(x, ux)))])
}



setTipoDoc <- function(Documento){
  Documento <- ifelse(Documento=="1","CC",
                      ifelse(Documento=="2","TI",
                             ifelse(Documento=="3","RC",
                                    ifelse(Documento=="4","CE",
                                           ifelse(Documento=="5","NUIP",
                                                  ifelse(Documento=="6","PAS",
                                                         ifelse(Documento=="7","NIT",
                                                                ifelse(Documento=="8","CD",
                                                                       ifelse(Documento=="9", "CV", NA)))))))))
  return(Documento)
}

setGenero <- function(genero){
  genero <- ifelse(genero=="1","M", "F")
  return(genero)
}

setTipoAportante <- function(Aportante){
  Aportante <- ifelse(Aportante == "1" | Aportante == "2" | Aportante == "3" | Aportante == "13", "DEPENDIENTES",
                      ifelse(Aportante == "4" | Aportante == "5" | Aportante == "11", "PENSIONADOS",
                            ifelse(Aportante == "6", "FACULTATIVOS",
                                  ifelse(Aportante == "7" | Aportante == "8" | Aportante == "12", "INDEPENDIENTES",
                                        ifelse(Aportante == "9" | Aportante == "14", "FIDELIDAD",
                                              ifelse(Aportante == "10", "DESAFILIADO CON DERECHO A SUBSIDIO", NA))))))
                                    
  return(Aportante)
}

setParentesco <- function(Documento){
  Documento <- ifelse(Documento=="1","HIJO",
                      ifelse(Documento=="2","PADRE",
                             ifelse(Documento=="3","HERMANO",
                                    ifelse(Documento=="4","HIJASTRO",
                                           ifelse(Documento=="5","CONYUGUE",
                                                  ifelse(Documento=="6","DEPENDIENTES POR CUSTODIA LEGAL O JUDICIAL", NA))))))
  return(Documento)
}

coincidencia<-function(n1="",n2="",rpta=0){
  #n1<-"gutierrez juan reina pablo ñ"
  #n2<-"juan pablo reina   n"
  n1<-str_squish(str_to_upper(n1))
  n2<-str_squish(str_to_upper(n2))
  #n1
  #n2
  #print(n1)
  #print(n2)
  #print(n1 == n2)
  if(n1==n2){
    total<-as.integer(100)
    #total
  }
  else{
    # str_squish(str_replace_all(n1,"Ñ","N"))
    # str_squish(str_replace_all(n2,"Ñ","N"))
    #print(str_squish(str_replace_all(n1,"Ñ","N")))
    #print(str_squish(str_replace_all(n2,"Ñ","N")))
    #print(str_squish(str_replace_all(n1,"Ñ","N"))==str_squish(str_replace_all(n2,"Ñ","N")))
    
    if(str_squish(str_replace_all(n1,"Ñ","N"))==str_squish(str_replace_all(n2,"Ñ","N"))){
      total<-as.integer(99)
    }
    else{
      total<-0
      
      #print(str_length(n1))
      #print(str_length(n2))
      
      if(str_length(n1)>0 & str_length(n2)>0){
        
        #print(str_split(n1," ",simplify=TRUE))
        #print(str_split(n2," ",simplify=TRUE))
        
        #print(length(str_split(n1," ",simplify=TRUE)))
        #print(length(str_split(n2," ",simplify=TRUE)))
        
        if(length(str_split(n1," ",simplify=TRUE))<length(str_split(n2," ",simplify=TRUE))){
          tmp<-n2
          n2<-n1
          n1<-tmp
        }
        
        p1<-str_split(str_replace(n1,"Ñ","N")," ")
        #p1
        
        #p1[[1]]
        p1[[2]]<-rep(0,length(p1[[1]]))
        #print(rep(0,length(p1[[1]])))
        #print(p1[[2]])
        #print(p1)
        names(p1)<-c("palabra","pos")
        #p1
        p2<-str_split(str_replace(n2,"Ñ","N")," ")
        p2[[2]]<-rep(0,length(p2[[1]])) # Crear un vector con n cantidad de ceros donde n es la cantidad de palabras
        names(p2)<-c("palabra","pos")
        porcentaje<-100/length(p1$palabra)
        
        #p1$palabra
        #p2$palabra
        
        for (i in 1:length(p1$palabra)){
          bandera<-0
          
          #print(p1$palabra)
          #print(p2$palabra[0])
          #print(p1$palabra[0] == p2$palabra[0])
          #p1
          for (j in 1:length(p2$palabra)){
            #print(paste(length(p1$palabra),length(p2$palabra),i,j,p1$palabra[i],p2$palabra[j],p1$pos[i],p2$pos[j]))
            if(p1$palabra[i]==p2$palabra[j] && p2$pos[j]==0 && bandera==0){
              p1$pos[i]<-1
              p2$pos[j]<-1
              total<-total+porcentaje
              bandera<-1
            }
          }
        }
      }
    }
  }
  if(rpta!=0){
    if(total>98){
      p1<-str_split(str_replace_all(n1,"Ñ","N")," ")
      p1[[2]]<-rep(1,length(p1[[1]]))
      names(p1)<-c("palabra","pos")
    }
    return(p1)
  }
  else
    return(as.integer(total))
}