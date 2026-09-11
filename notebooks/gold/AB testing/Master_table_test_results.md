AB Test: PySpark (Broadcast) vs SQL (Table) for Gold Master Table Creation
Tried to figure out what coding language was faster and was suprised that to find that even with broadcast used in pyspark the sql version was almost 2x faster even without broadcast functions.



 The SQL speed test was 9.77 seconds while the pyspark speed test was 18.15 seconds. 
 
 
 
 Possible reasons can be that SQL gets parsed internally while python uses and interpreter using the  Py4J library  before the JVM engine. Also since SQL version didn't use hardcoded broadcast function the catalyst optimiser in databricks basically  does auto-broadcast if and when needed speeding up the code. 
 
 
 
  But ultimately the most speed improvement is due to customer.geo and seller.geo was scanning the gelocation so CTE was created to reduce it to one scan.
