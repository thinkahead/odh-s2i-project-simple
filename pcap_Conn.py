class pcapConn:
    import os
#    sparkServiceUrl='spark://spark-mngr-svc.spark-cluster.svc:7077'
    sparkServiceUrl=os.environ.get('SPARK_MNGR_URL', None)
    S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL', None)
    AWS_ACCESS_KEY_ID =os.environ.get('AWS_ACCESS_KEY_ID', None)
    AWS_SECRET_ACCESS_KEY=os.environ.get('AWS_SECRET_ACCESS_KEY', None)
    BUCKET_NAME=os.environ.get('BUCKET_NAME', None)
    
    
    def __init__(self, s3_endpoint_url=S3_ENDPOINT_URL, s3_access_key_id=AWS_ACCESS_KEY_ID, 
                       s3_secret_access_key=AWS_SECRET_ACCESS_KEY, s3_bucket=BUCKET_NAME,
                       groupNdx=0):
        self.s3_endpoint_url = s3_endpoint_url
        self.s3_access_key_id = s3_access_key_id
        self.s3_secret_access_key = s3_secret_access_key
        self.s3_bucket = s3_bucket
        self.groupNdx = groupNdx
        
        ## Need to use hostIP for all callbacks from workers to driver
        import socket
        self.hostname=socket.gethostname()
        self.hostip=socket.gethostbyname(socket.gethostname())

    def getSession(self):
        from pyspark.sql import SparkSession
        if pcapConn.sparkServiceUrl:
            self.spark = SparkSession.builder.appName("Spark-tek-RDD")\
            .config("spark.driver.host", self.hostip)\
            .config('spark.executor.memory', '64g')\
            .master(self.sparkServiceUrl)\
            .getOrCreate()
        else:
            self.spark = SparkSession.builder.appName("Spark-tek-RDD")\
            .getOrCreate()
        

        return self
        
    def configureS3FS(self):
        self.getSession()
        hadoopConf = self.spark.sparkContext._jsc.hadoopConfiguration()
        hadoopConf.set("fs.s3a.endpoint", self.s3_endpoint_url)
        hadoopConf.set("fs.s3a.access.key", self.s3_access_key_id)
        hadoopConf.set("fs.s3a.secret.key", self.s3_secret_access_key)
        hadoopConf.set("fs.s3a.path.style.access", "true")
        hadoopConf.set("fs.s3a.connection.ssl.enabled", "false")
        return self
    
    def loadRDD(self, filePattern="/RPE004/batch1/stream3.part01.pcap/7?????.tek"):
        self.configureS3FS()
        text_RDD = self.spark.sparkContext.textFile("s3a://" + self.s3_bucket + filePattern)
        
        self.pcap = self.spark.read.json(text_RDD)
        return self
    
    def getBucket(self):
        return self.s3_bucket
    
    def persist(self, df, folder):
        from datetime import datetime
        #now = datetime.now().strftime("%Y%m%d%H%M%S")
        return df.write.parquet("s3a://" + self.s3_bucket + "/"+folder+"/" + str(self.groupNdx), mode="append")

    def read(self, folder):
        return self.spark.read.parquet("s3a://" + self.s3_bucket + "/"+folder+ "/"+str(self.groupNdx)+ "/*")
    
    def getConnectionsList(self, tag=None):
        from pyspark.sql.functions import countDistinct, desc
        df = self.pcap.groupBy('source','destination','protocol').agg(countDistinct('no_').alias('count')).sort(desc('count'))
        if tag: self.persist(df, tag)
        return df
    
    def storeConnectionsList(self, tag='connections'):
        from pyspark.sql.functions import countDistinct, desc
        df = self.pcap.groupBy('source','destination','protocol').agg(countDistinct('no_').alias('count')).sort(desc('count'))
        return self.persist(df, tag)
    
    def readConnectionsList(self):
        return self.read('connections')
    
    def getConnectionsLength(self, tag=None):
        from pyspark.sql.functions import countDistinct, desc, min, max, avg
        df = self.pcap.groupBy('source','destination','protocol')\
            .agg(countDistinct('no_').alias('count'), min("length"),max("length"), avg("length"))\
            .sort(desc('count'))
        if tag: self.persist(df, tag)
        return df

    def storeConnectionsLength(self, tag="connections-length"):
        from pyspark.sql.functions import countDistinct, desc, min, max, avg
        df = self.pcap.groupBy('source','destination','protocol')\
            .agg(countDistinct('no_').alias('count'), min("length"),max("length"), avg("length"))\
            .sort(desc('count'))
        return self.persist(df, tag)

    def readConnectionsLength(self):
        return self.read('connections-length')
    
    @staticmethod
    def decodeConnections(argv):
        import inspect
        print ("Called method: ", inspect.currentframe().f_code.co_name)
        pcap= pcapConn(groupNdx=argv[1]) if len(argv) > 1 else pcapConn()
        pcap=pcap.loadRDD(filePattern=argv[0]) if len(argv) > 0 else pcap.loadRDD()
        pcap.storeConnectionsList()
        return pcap

        
    @staticmethod
    def decodeConnLength(argv):
        import inspect
        print ("Called method: ", inspect.currentframe().f_code.co_name)
        pcap= pcapConn(groupNdx=argv[1]) if len(argv) > 1 else pcapConn()
        pcap=pcap.loadRDD(filePattern=argv[0]) if len(argv) > 0 else pcap.loadRDD()
        pcap.storeConnectionsLength()
        return pcap

        
    def __del__(self):
        self.spark.stop()
    
# usage:
# python pcap_Conn.py <file pattern> <group Ndx>
# python pcap_Conn.py /RPE004/batch1/stream3.part01.pcap/7?????.tek 7

if __name__ == "__main__":
#    import os
#    for k in os.environ: print (k,'=',os.environ[k])
    import sys
    print (sys.argv)
#    pcapConn.decodeConnections(sys.argv[1:])
    pcapConn.decodeConnLength(sys.argv[1:])

        
