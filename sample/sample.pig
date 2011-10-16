register ./jar/myudfs.jar
set job.priority very_low;
set job.name 'GetReqParamSample';
define GET com.gsd.pig.udf.GetReqParam();
a = load 'SampleData' using PigStorage() as request;
b = foreach a generate GET(request,'url') as url;
c = filter b by url != '';
store c into 'GetReqParamSample';
copyToLocal GetReqParamSample /path/for/local;

