const mqtt = require("mqtt");
const { InfluxDB, Point } = require('@influxdata/influxdb-client')

const token = process.env.TOKEN
const url = process.env.URL
const org = process.env.ORG;
const bucket = process.env.BUCKET

//setting up influxdb client
const client = new InfluxDB({ url, token });

//setting up mqtt client
const clients = mqtt.connect(process.env.MQ_URL, {
    username: process.env.USER,
    password: process.env.PASS,
    port: process.env.PORT
})

clients.on("connect", async () => {
    console.log("connected");
    await clients.publish(bucket, JSON.stringify({ bucket, value: "change-me" }));  //publishing data to mqtt
});

clients.subscribe(process.env.BUCKET);//subscribing to mqtt

clients.on("message", (topic, message) => {
    console.log("message received", topic, message.toString());
    const data = JSON.parse(message.toString());
    console.log(data);//printing the data

    const writeClient = client.getWriteApi(org, bucket, 'ns'); //setting up write client
    //creating point
    let point = new Point('names').tag(Object.keys(data)[0], Object.values(data)[0])
        .tag(Object.keys(data)[1], Object.values(data)[1])
        .stringField(org, "hello");
    writeClient.writePoint(point);//writing point to influxdb
    writeClient.flush();//flushing the data to influxdb
    console.log(point);

});

//seeing the data in influxdb
let query = client.getQueryApi(org)
let fluxquery = `form(bucket:"${bucket}")
|>  range(start:-100m)
|>  filter(fn:(r)=>r._measurement=="names")`

query.queryRows(fluxquery, {
    next: (row, tableMeta) => {
        const tableObject = tableMeta.toObject(row)
        console.log(row);
        console.log(tableObject)
    },
    error: (error) => {
        console.error('\nError', error)
    },
    complete: () => {
        console.log('\nSuccess for Query1')
    }
})
