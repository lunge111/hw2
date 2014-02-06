library(AirlineDelays)
library(parallel)
cl <- makeCluster(detectCores(),"FORK")
clusterSetRNGStream(cl)
g=function(i){ setwd("~/data/assignment1")
            filename=dir()
           if(i<22) 
            a=read.csv(filename[i],
                       colClasses=c(rep("NULL",14), "character", rep("NULL",14)))
            else
             a= read.csv(filename[i],
                       colClasses=c(rep("NULL",42), "character", rep("NULL",66)))
           a=table(a,na.rm=FALSE)
           a=as.table(a)
}

mergetable<-function(a,b){ #merge 2 tables
  k=as.data.frame(a)
  l=as.data.frame(b)
  V=merge(k,l, by.x='a', by.y='b',all=TRUE)
  V[is.na(V)]=0
  V$Freq.x=V[2]+V[3]
  V[3]=NULL
  V
}

k<-function(df){  a=0
                  i=1
                  x=sort(df[,2])/sum(df[,2])
                  while(a<=0.5){  
                    a=a+x[i]
                    i=i+1
                  }
                  df[i,1]
}

m<-function(cl){
tables=clusterApply(cl, 1:81, g)
V=mergetable(tables[[1]],tables[[2]])
for(i in 3:81){
  V=merge(V,as.data.frame(tables[[i]]),by.x='a',by.y='a',all=TRUE)
  V$Freq.x[is.na(V$Freq.x)]=0
  V$Freq[is.na(V$Freq)]=0
  V$Freq.x=V[2]+V[3]
  V[3]=NULL
}
del=as.numeric(V[,1])
fre=V[,2]
total = sum(fre)
ns = fre/total
c(sum(ns*del),sd=sqrt(sum((del-sum(del * ns))^2 * ns)))
}
system.time(a<-m(cl))

