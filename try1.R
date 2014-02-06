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
}
time1=system.time(datalist<-clusterApply(cl, 1:81, g))  #get time of loading data
result1<-function(datalist) { #this function is to calculate mean and s.d.  
  mean=rep(0,81)         #use Welford's updating formula
  s2=rep(0,81)
  a=rep(0,81)
  
  for(m in 1:21){ 
    delay1=as.integer(datalist[[m]][!is.null(datalist[[m]]$ArrDelay),"ArrDelay"])
    delay=delay1[!is.na(delay1)]
    a[m]=length(delay)
    for(n in 1:length(delay)){
      d=delay[n]-mean[m]
      mean[m]=mean[m]+d/n
      s2[m]=s2[m] + d*(delay[n] - mean[m])    
      
    }
  }
  for(m in 22:81){
    
    delay1=as.integer(datalist[[m]][!is.null(datalist[[m]]$ARR_DELAY),"ARR_DELAY"])
    delay=delay1[!is.na(delay1)]
    a[m]=length(delay)
    for(n in 1:length(delay)){
      d=delay[n]-mean[m]
      mean[m]=mean[m]+d/n
      s2[m]=s2[m] + d*(delay[n] - mean[m])    
      
    }
  }
  m=sum(mean*a)/sum(a) 
  
  
  
  
  s=sqrt(sum(s2)/sum(a)) 
  c(m,s)
}
time2=system.time(stew<-result1(datalist)) #get time of running result1 function
summary=list(loadfiletime = time1,runtime=time2, results = c(mean = stew[1], sd = stew[2]),
             system = Sys.info(),  session = sessionInfo() )
save(summary,file="result1.rda")
