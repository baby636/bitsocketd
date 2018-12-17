// Input: ZMQ
const zmq = require("zeromq")
const mingo = require("mingo")
const bcode = require("bcode")
const jq = require("bigjq")
const Agenda = require('agenda')
const defaults = {
  zmq: { host: "127.0.0.1", port: 28339 },
  mongo: { host: "127.0.0.1", port: 27020 }
}
/*
config := {
  zmq: {
    host: [host],
    port: [port]
  },
  mongo: {
    host: [host],
    port: [port]
  }
}
*/
var mysql = require('mysql');

function recordMempoolNotification(mysql_conn, result, str_status) {
    result.out.forEach(function(out) {
        if (out.e.a) {
            let addSql = 'INSERT INTO notify_history(Id,txid,sub_address,create_time,status) VALUES(0,?,?,?,?)';
            // console.log('result=',result)
            let addSqlParams = [result['tx']['h'], out.e.a, Math.floor(new Date() / 1000), str_status];

            mysql_conn.query(addSql,addSqlParams,function (err, result) {
                if(err){
                    console.log('[INSERT ERROR] - ', err.message);
                    return;
                }

                console.log('-------INSERT------');
                console.log('INSERT ID:', result);
                console.log('-----------------------------------\n\n');
            });
        }
    })

}

const init = async function(config) {
  let sock = zmq.socket("sub")
  let concurrency = (config.concurrency && config.concurrency.mempool ? config.concurrency.mempool : 1)
  let zmqhost = ((config.zmq && config.zmq.host) ? config.zmq.host : defaults.zmq.host)
  let zmqport = ((config.zmq && config.zmq.port) ? config.zmq.port : defaults.zmq.port)
  let mongohost = ((config.mongo && config.mongo.host) ? config.mongo.host : defaults.mongo.host)
  let mongoport = ((config.mongo && config.mongo.port) ? config.mongo.port : defaults.mongo.port)
  let connections = config.connections
  const agenda = new Agenda({
    db: {address: mongohost + ":" + mongoport + "/agenda" },
    defaultLockLimit: 1000,
    maxConcurrency: 1,
  })

  var mysql_conn = mysql.createConnection({
    host: config.mysql.host,
    user: config.mysql.user,
    password: config.mysql.password,
    database: config.mysql.database,
  });

  agenda.define('SSE', (job, done) => {
    let data = job.attrs.data
    console.log("JOB = ", data)
    let o = data.o
    let tx = JSON.parse(o)

    let promises = Object.keys(connections.pool).map(function(key) {
      let connection = connections.pool[key]
      return new Promise(function(resolve, reject) {
        console.log("3. Encoding")
        const encoded = bcode.encode(connection.query)
        const types = encoded.q.db
        if (!types || types.indexOf("u") >= 0) {
          let filter = new mingo.Query(encoded.q.find)
          if (filter.test(tx)) {
            console.log("4. Decoding")
            let decoded = bcode.decode(tx)
            try {
              if (encoded.r && encoded.r.f) {
                console.log("5. Running")
                jq.run(encoded.r.f, [decoded]).then(function(result) {
                  console.log("6. Finished Running")

                  try {
                    connection.res.sseSend({ type: "mempool", data: result })
                    recordMempoolNotification(mysql_conn, result, 'success')

                  } catch (e) {
                      /* handle error */
                    recordMempoolNotification(mysql_conn, result, 'fail')
                  }

                  resolve()
                })
                .catch(function(err) {
                  console.log("#############################")
                  console.log("## Error")
                  console.log(err)
                  console.log("## Processor:", encoded.r.f)
                  console.log("## Data:", [decoded])
                  console.log("#############################")
                  resolve()
                })
              } else {
                console.log("5. No Running")
                result = [decoded]
                console.log("6. Finished No Running")
                try {
                  connection.res.sseSend({ type: "mempool", data: result })
                  recordMempoolNotification(mysql_conn, result, 'success')

                } catch (e) {
                    /* handle error */
                  recordMempoolNotification(mysql_conn, result, 'fail')
                }
                resolve()
              }
            } catch (e) {
              console.log("Error", e)
              resolve()
            }
          } else {
            resolve()
          }
        } else {
          resolve()
        }
      })
    })
    Promise.all(promises).then(function() {
      console.log("## Finished", o)
      done()
    })
  })
  await agenda.start()
  console.log("Job queue started")


  sock.connect("tcp://" + zmqhost + ":" + zmqport)
  sock.subscribe("mempool")
  sock.subscribe("block")
  sock.on("message", function(topic, message) {
    let type = topic.toString()
    let o = message.toString()
    switch (type) {
      case "mempool": {
        console.log("1. Mempool")
        agenda.now("SSE", { o: o })
        break
      }
      case "block": {
        let block = JSON.parse(o)
        Object.keys(connections.pool).forEach(async function(key) {
          let connection = connections.pool[key]
          const encoded = bcode.encode(connection.query)
          const types = encoded.q.db
          if (!types || types.indexOf("c") >= 0) {
            let filter = new mingo.Query(encoded.q.find)
            let filtered = block.txs.filter(function(tx) {
              return filter.test(tx)
            })
            let transformed = []
            for(let i=0; i<filtered.length; i++) {
              let tx = filtered[i]
              let decoded = bcode.decode(tx)
              let result
              try {
                if (encoded.r && encoded.r.f) {
                  result = await jq.run(encoded.r.f, [decoded])
                } else {
                  result = decoded
                }
                transformed.push(result)
              } catch (e) {
                console.log("Error", e)
              }
            }
            connection.res.sseSend({
              type: "block", index: block.i, data: transformed
            })
          }
        })
        break
      }
    }
  })
}
module.exports = { init: init }
