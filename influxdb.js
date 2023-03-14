//setting up mqtt with influx db 

const mqtt = require("mqtt");

const { InfluxDB, Point } = require('@influxdata/influxdb-client')

const token = process.env.TOKEN

const url = process.env.URL
const org='nrxen';
const bucket='nimmos'
//setting up influxdb client
let client=new InfluxDB({url,token});
//setting up mqtt client
const clients=mqtt.connect(process.env.MQ_URL,{
    username: process.env.USER,
    password: process.env.PASS,
    port: process.env.PORT
})
clients.on("connect", async () => {
     console.log("connected");
    await clients.publish("nimmos", JSON.stringify({name:"nimmos",value:"route-engine"}));  //publishing data to mqtt
});

 clients.subscribe("nimmos");//subscribing to mqtt

 clients.on("message", (topic, message) => {
    console.log("message received", topic, message.toString());
    const data=JSON.parse(message.toString());
    console.log(data);//printing the data
    
    const writeClient = client.getWriteApi(org, bucket,'ns'); //setting up write client
    //creating point
    let point=new Point('names').tag(Object.keys(data)[0],Object.values(data)[0])
    .tag(Object.keys(data)[1],Object.values(data)[1])
    .stringField('nrxen',"hello");
    writeClient.writePoint(point);//writing point to influxdb
    writeClient.flush();//flushing the data to influxdb
    console.log(point);

});
//seeing the data in influxdb
let query=client.getQueryApi(org)
let fluxquery=`form(bucket:"nimmos")
|>  range(start:-1m)
|>  filter(fn:(r)=>r._measurement=="names")`

query.queryRows(fluxquery,{
    next:(row,tableMeta)=>{
        const tableObject = tableMeta.toObject(row)
        console.log(row);
        console.log(tableObject)
    },
    error:(error)=>{
        console.error('\nError',error)
    },
    complete:()=>{
        console.log('\nSuccess for Query1')
    }

})
