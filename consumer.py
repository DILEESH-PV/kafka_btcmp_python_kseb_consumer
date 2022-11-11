from kafka import KafkaConsumer
import mysql.connector
import ast
try:
    mydb=mysql.connector.connect(host='localhost',user='root',password='',database='ksebdb')
except mysql.connector.Error as e:
    print(e)
mycursor=mydb.cursor()

bootstrap_server = ["localhost:9092"]

topic = "kseb"

consumer = KafkaConsumer(topic, bootstrap_servers = bootstrap_server)

for i in consumer:
    print(str(i.value.decode()))
    result=ast.literal_eval(i.value.decode())
    userid=(result.get('userid'))
    unit=(result.get('unit'))
    # print(userid)
    # print(unit)
    sql="INSERT INTO `usages`(`consumerid`, `unit`, `date`) VALUES (%s,%s,now())"
    #sql = 'INSERT INTO `usages`(`consumerid`, `unit`, `date`) VALUES (%s,%s,now())'
    data = (userid,unit)
    mycursor.execute(sql,data)
    mydb.commit()
    print("Data inserted successfully.")
    