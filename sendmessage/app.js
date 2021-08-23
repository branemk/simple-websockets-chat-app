// Copyright 2018-2020Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: MIT-0

const AWS = require('aws-sdk');

const jwt_decode = require("jwt-decode");
const ddb = new AWS.DynamoDB.DocumentClient({ apiVersion: '2012-08-10', region: process.env.AWS_REGION });
const sqs = new AWS.SQS({ apiVersion: '2012-11-05' });
const { TABLE_NAME } = process.env;

exports.handler = async event => {
  let connectionData;
  console.log("Domain name",event.requestContext.domainName + '/' + event.requestContext.stage);
  try {
    connectionData = await ddb.scan({ TableName: TABLE_NAME, ProjectionExpression: 'connectionId' }).promise();
  } catch (e) {
    return { statusCode: 500, body: e.stack };
  }
  
  const apigwManagementApi = new AWS.ApiGatewayManagementApi({
    apiVersion: '2018-11-29',
    endpoint: event.requestContext.domainName + '/' + event.requestContext.stage
  });
  const defaultCMD = async (event,connectionData,postData) => {
    const postCalls = connectionData.Items.map(async ({ connectionId }) => {
        try {
          await apigwManagementApi.postToConnection({ ConnectionId: connectionId, Data: postData }).promise();
        } catch (e) {
          if (e.statusCode === 410) {
            console.log(`Found stale connection, deleting ${connectionId}`);
            await ddb.delete({ TableName: TABLE_NAME, Key: { connectionId } }).promise();
          } else {
            throw e;
          }
        }
      });
      
      try {
        await Promise.all(postCalls);
        return { statusCode: 200, body: 'Data sent.' };
      } catch (e) {
        return { statusCode: 500, body: e.stack };
      }
  }
  const addCmdToQueue = async (event,cmd,token, equipment) => {
    console.log("name",equipment.name);
    console.log("id",equipment.equipmentId);
    const decoded = jwt_decode(token);
    let putParams = {
      TableName: "InsightCommandQueue",
      Item: {
        id : event.requestContext.requestId + new Date().getTime(),
        connection_id: event.requestContext.connectionId,
        cmd : cmd,
        equipmentId : equipment.equipmentId,
        status : "PENDING",
        checkQueue : false,
        email : decoded.email,
        date : new Date().getTime(),
        equipmentName : equipment.name,
        name : equipment.name,
        equipment : equipment.definition,
        notificationRead:false
      }
    };
    
    console.log("params",putParams)
  
    try {
      await ddb.put(putParams).promise();
      delete putParams.Item.equipment;
      putParams.Item.type = putParams.Item.cmd;
      delete putParams.Item.cmd;
      return putParams.Item;
    } catch (err) {
      console.log("err",err)
      return { statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(err) };
    }
  } 
  const addPdfCmdToQueue = async (event,cmd,token, pdf) => {
    console.log("name",equipment.name);
    console.log("id",equipment.equipmentId);
    const decoded = jwt_decode(token);
    let putParams = {
      TableName: "InsightCommandQueue",
      Item: {
        id : event.requestContext.requestId + new Date().getTime(),
        connection_id: event.requestContext.connectionId,
        cmd : cmd,
        status : "PENDING",
        checkQueue : false,
        email : decoded.email,
        date : new Date().getTime(),
        pdf : pdf,
        notificationRead:false
      }
    };
    
    console.log("params",putParams)
  
    try {
      await ddb.put(putParams).promise();
      delete putParams.Item.equipment;
      putParams.Item.type = putParams.Item.cmd;
      delete putParams.Item.cmd;
      return putParams.Item;
    } catch (err) {
      console.log("err",err)
      return { statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(err) };
    }
  } 

  const addPendingNotification = async (id,event,cmd,token, equipment) => {
    console.log("name",equipment.name);
    console.log("id",equipment.equipmentId);
    const decoded = jwt_decode(token);
    let putParams = {
      TableName: "InsightNotificationHistory",
      Item: {
        id,
        connection_id: event.requestContext.connectionId,
        cmd : cmd,
        equipmentId : equipment.equipmentId,
        status : "PENDING",
        checkQueue : false,
        email : decoded.email,
        date : new Date().getTime(),
        equipmentName : equipment.name,
        name : equipment.name,
        equipment : equipment.definition,
        notificationRead:false
      }
    };
    
    console.log("params",putParams)
  
    try {
      await ddb.put(putParams).promise();
      delete putParams.Item.equipment;
      putParams.Item.type = putParams.Item.cmd;
      delete putParams.Item.cmd;
      return putParams.Item;
    } catch (err) {
      console.log("err",err)
      return { statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(err) };
    }
  } 

  const addPdfPendingNotification = async (id,event,cmd,token, pdf) => {
    const decoded = jwt_decode(token);
    let putParams = {
      TableName: "InsightNotificationHistory",
      Item: {
        id,
        connection_id: event.requestContext.connectionId,
        cmd : cmd,
        equipmentId : equipment.equipmentId,
        status : "PENDING",
        checkQueue : false,
        email : decoded.email,
        date : new Date().getTime(),
        pdf : pdf,
        notificationRead:false
      }
    };
    
    console.log("params",putParams)
  
    try {
      await ddb.put(putParams).promise();
      delete putParams.Item.equipment;
      putParams.Item.type = putParams.Item.cmd;
      delete putParams.Item.cmd;
      return putParams.Item;
    } catch (err) {
      console.log("err",err)
      return { statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(err) };
    }
  } 

  const addCmdToQueueACK = async (event,cmd,token, alarmId,name,tenantId) => {
    const decoded = jwt_decode(token);
    let putParams = {
      TableName: "InsightCommandQueue",
      Item: {
        id : event.requestContext.requestId + new Date().getTime(),
        connection_id: event.requestContext.connectionId,
        cmd : cmd,
        status : "PENDING",
        checkQueue : false,
        email : decoded.email,
        date : new Date().getTime(),
        alarmId,
        notificationRead:false,
        name,
        tenantId
      }
    };
    
    console.log("params",putParams)
  
    try {
      await ddb.put(putParams).promise();
      delete putParams.Item.equipment;
      putParams.Item.type = putParams.Item.cmd;
      delete putParams.Item.cmd;
      return putParams.Item;
    } catch (err) {
      console.log("err",err)
      return { statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(err) };
    }
  } 

  const addPendingNotificationACK = async (id,event,cmd,token, alarmId,name,tenantId) => {
    const decoded = jwt_decode(token);
    const exist = await getACKQueueForUser(decoded.email,alarmId);
    if(exist.length > 0){
      for (const arn of exist) {
        await ddb.delete({ TableName: "InsightCommandQueue", Key: { id } }).promise();
      }
      return;
    }
    let putParams = {
      TableName: "InsightNotificationHistory",
      Item: {
        id,
        connection_id: event.requestContext.connectionId,
        cmd : cmd,
        status : "PENDING",
        checkQueue : false,
        email : decoded.email,
        date : new Date().getTime(),
        alarmId,
        notificationRead:false,
        name,
        tenantId
      }
    };
    
    console.log("params",putParams)
  
    try {
      await ddb.put(putParams).promise();
      delete putParams.Item.equipment;
      putParams.Item.type = putParams.Item.cmd;
      delete putParams.Item.cmd;
      return putParams.Item;
    } catch (err) {
      console.log("err",err)
      return { statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(err) };
    }
  } 
  const getACKQueueForUser =  async(email, alarmId) =>{
    var params2 = {
      ExpressionAttributeValues: {
        ":e": `PENDING`,
        ":arm": `${alarmId}`
      },
      ExpressionAttributeNames: {
                  "#customStatus": "status"
              },
      FilterExpression: "#customStatus = :e AND alarmId = :arm",
      TableName: "InsightNotificationHistory"
    };
    return new Promise(resolve => {
      ddb.scan(params2, function (err,data) {
        console.log("err getACKQueueForUser", err)
        console.log("Items getACKQueueForUser", data)
        resolve(data.Items.filter(r => r.status === "PENDING"));
      });
    })
  }
  

  const getCurrentCMDforUser = async (email) => {
    var params2 = {
      ExpressionAttributeValues: {
       ":e":  `${email}`,
       ":ce":   false
      }, 
      FilterExpression: "email = :e AND checkQueue = :ce", 
      TableName: "InsightCommandQueue"
     };
      return new Promise(resolve => {
          ddb.scan(params2, function(err, data){
              console.log("err",err)
          console.log("Items",data)
          resolve(data.Items);
      });
      })
  }

  const getUnreadCMDforUser = async (email) => {
    var params2 = {
      ExpressionAttributeValues: {
       ":e":  `${email}`,
       ":ce":   false
      }, 
      FilterExpression: "email = :e AND checkQueue = :ce", 
      TableName: "InsightCommandQueue"
     };
      return new Promise(resolve => {
          ddb.scan(params2, function(err, data){
              console.log("err",err)
          console.log("Items",data)
          resolve(data.Items);
      });
      })
  }
  const getCompletedQueueforUser = async (email, readFilter = true) => {
    var params = {
      ExpressionAttributeValues: {
       ":e":  `${email}`,
       ":nr" : false
      }, 
      FilterExpression: "email = :e AND notificationRead = :nr", 
      TableName: "InsightNotificationHistory"
     };
     if(readFilter) {delete params.ExpressionAttributeValues[":nr"]; params.FilterExpression = "email = :e";}
      return new Promise(resolve => {
          ddb.scan(params, function(err, data){
              console.log("err",err)
          console.log("Items",data)
          resolve(data.Items);
      });
      })
  }
  const ckeckQueue = async (event,cmd,token) => {
    const decoded = jwt_decode(token);
    const email = decoded.email;
    let currentQueue = await getCurrentCMDforUser(email);
    let completedQueue = await getCompletedQueueforUser(email);
      const putParams = {
        TableName: "InsightCommandQueue",
        Item: {
          id: event.requestContext.requestId + new Date().getTime(),
          connection_id: event.requestContext.connectionId,
          cmd : "*", // TODO : change for more cmd in the future
          equipmentId : "test",
          status : "PENDING",
          checkQueue : true,
          email : decoded.email,
          date : new Date().getTime(),
        }
      };
      
      console.log("params",putParams)
    
      try {
        await ddb.put(putParams).promise();
        // currentQueue.push(putParams.Item)
        // return { statusCode: 200, body: 'Data sent.' };
      } catch (err) {
        console.log("err",err)
        return { statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(err) };
      }
    currentQueue = currentQueue.map(d => {delete d.equipment; return d;})
    completedQueue = completedQueue.map(d => {delete d.equipment; return d;})
    return [...currentQueue,...completedQueue];
  } 
  
  const markNotificationsAsRead = async (id) => {
    console.log("read",id)
    const params = {
      TableName: "InsightCommandQueue",
      Key: {
          "id": id
      },
      UpdateExpression: "set notificationRead = :x",
      ExpressionAttributeValues: {
          ":x": true,
      }
  };

  return new Promise( resolve => {
    ddb.update(params, function(err, data) {
      if (err) console.error(err);
      else console.log(data);
      resolve(data);
  });
  })
  }
  
  const markCompletedNotificationsAsRead = async (id) => {
    console.log("read",id)
    const params = {
      TableName: "InsightNotificationHistory",
      Key: {
          "id": id
      },
      UpdateExpression: "set notificationRead = :x",
      ExpressionAttributeValues: {
          ":x": true,
      }
  };

  return new Promise( (resolve,reject) => {
    ddb.update(params, function(err, data) {
      if (err) reject(err);
      else console.log(data);
      resolve(data);
  });
  })
  }

  

  const checkNotifications = async (event,cmd,token) => {
    const decoded = jwt_decode(token);
    const email = decoded.email;
    let currentQueue = await getUnreadCMDforUser(email);
    let completedQueue = await getCompletedQueueforUser(email,false);

    console.log("Current Queue",currentQueue);
    console.log("Current Completed Queue",completedQueue);
    for (const record of currentQueue) {
      await markNotificationsAsRead(record.id)
    }
    for (const record of completedQueue) {
      try {
        await markCompletedNotificationsAsRead(record.id)
      } catch (error) {
        console.error(error);
        continue;
      }
    }
    return {done : true};
  }
  const sendEventToQueue = async (email) => {
    console.log("Email",email);
    // Filter DynamoDB
    var params2 = {
          ExpressionAttributeValues: {
           ":e":  `${email}`
          }, 
          FilterExpression: "email = :e", 
          TableName: "InsightCommandQueue"
         };
    return new Promise(resolve => {
        ddb.scan(params2, function(err, data){
            console.log("err",err)
    console.log("Items",data)
        resolve(data);
    });
    })
    
};
  const sendNotificationUpdate = async (token) => {
    const decoded = jwt_decode(token);
    const email = decoded.email;
    const items = await sendEventToQueue(email);
    const sended = []
    console.log("sendNotificationUpdate",{items,email})
    try {
      for(const {id,cmd,email,connection_id,status} of items.Items){
          // let currentQueue = await getCurrentCMDforUser(email);
          // currentQueue = currentQueue.filter(q => (q.status !== "COMPLETED"));
          let currentQueue = [];
          let completedQueue = await getCompletedQueueforUser(email);
          completedQueue = completedQueue.map(q => { if(q.equipment)delete q.equipment; return q;});
          console.log("sendNotificationUpdate currentQueue",currentQueue)
          console.log("sendNotificationUpdate completedQueue",completedQueue);
          console.log(`Sending socket ${connection_id}`)
          if(sended.includes(connection_id)) continue;
          try {
            await apigwManagementApi.postToConnection({ ConnectionId: connection_id, Data: JSON.stringify({ type : "NOTIFICATION_UPDATE", queue : [...currentQueue,...completedQueue]}) }).promise();  
            console.log(`Sent ${connection_id}, queue : ${[...currentQueue,...completedQueue]}`);
            sended.push(connection_id);
          } catch (e) {
            console.log(`Error ${connection_id}`,e);
              if (e.statusCode === 410) {
                console.log(`Found stale connection, deleting ${connection_id}`);
                await ddb.delete({ TableName: "InsightCommandQueue", Key: { id } }).promise();
              } else {
                //throw e;
              }
          }
        }
      } catch (error) {
        console.log(`error socket ${error}`)

        //throw Error(error);
      }

      return "Done";
  }
  const postData = JSON.parse(event.body).data;
  console.log("event" ,postData);
  switch (postData.cmd) {
    case "get_connection_id":
       await apigwManagementApi.postToConnection({ ConnectionId: event.requestContext.connectionId, Data: JSON.stringify({ type : "connectionId", connectionId : event.requestContext.connectionId}) }).promise();
      break;
      case "EDIT_ED":
        try {
          const record = await addCmdToQueue(event,postData.cmd,postData.token,postData.equipment);
          await addPendingNotification(record.id,event,postData.cmd,postData.token,postData.equipment);
          // await apigwManagementApi.postToConnection({ ConnectionId: event.requestContext.connectionId, Data: JSON.stringify(record) }).promise();
          await sendNotificationUpdate(postData.token);
        } catch (error) {
          throw Error({ statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(error) });
        }
      break;
      case "PDF_REPORT":
        try {
          const record = await addPdfCmdToQueue(event,postData.cmd,postData.token,postData.url);
          await addPdfPendingNotification(record.id,event,postData.cmd,postData.token,postData.url);
          await sentSQSPDFReport(postData.cmd,postData.url)
          await sendNotificationUpdate(postData.token);
        } catch (error) {
          throw Error({ statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(error) });
        }
      break;
      case "CURRENT_QUEUE":
        try {
          const cuurentQueue = await ckeckQueue(event,postData.cmd,postData.token);
          await apigwManagementApi.postToConnection({ ConnectionId: event.requestContext.connectionId, Data: JSON.stringify({ type : "CURRENT_QUEUE", queue : cuurentQueue}) }).promise();

        } catch (error) {
          throw Error({ statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(error) });
        }
      break;
      case "READ_NOTIFICATIONS":
        try {
          const readResult = await checkNotifications(event,postData.cmd,postData.token);
          console.log("SENDING READ")
          await sendNotificationUpdate(postData.token);
          console.log("SENDING READ DONE")
        } catch (error) {
          throw Error({ statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(error) });
        }
      break;   
      case "CONTROL_ALARM_ACK":
        console.log("ONTROL_ALARM_ACK")
        const decoded = jwt_decode(postData.token);
        const email = decoded.email;
        console.log("email",email);
        const alarms = postData.alarms;
        let groupedByGateway = {};
        let groupedByGatewayAlarmIds = {};
        let groupedByAlarmName = {};
        for (const alarm of alarms) {
          if (!groupedByGateway[alarm.serialNumber]) groupedByGateway[alarm.serialNumber] = [];
          groupedByGateway[alarm.serialNumber].push(alarm.alarmId);

          if (!groupedByGatewayAlarmIds[alarm.serialNumber]) groupedByGatewayAlarmIds[alarm.serialNumber] = [];
          groupedByGatewayAlarmIds[alarm.serialNumber].push(alarm.alarm);

          if (!groupedByAlarmName[alarm.alarm]) groupedByAlarmName[alarm.alarm] = "";
          groupedByAlarmName[alarm.alarm] = alarm.name;
        }
        for (const [serialNumber,alarms] of Object.entries(groupedByGateway)) {
          for (const arm of groupedByGatewayAlarmIds[serialNumber]) {
            const record = await addCmdToQueueACK(event,postData.cmd,postData.token,arm,groupedByAlarmName[arm],postData.tenantId);
            await addPendingNotificationACK(record.id,event,postData.cmd,postData.token,arm,groupedByAlarmName[arm],postData.tenantId);
            await apigwManagementApi.postToConnection({ ConnectionId: event.requestContext.connectionId, Data: JSON.stringify(record) }).promise();
            await sendNotificationUpdate(postData.token);
            } 
            try {
              var params = {
                // Remove DelaySeconds parameter and value for FIFO queues
                DelaySeconds: 10,
                MessageAttributes: {
                  "Title": {
                    DataType: "String",
                    StringValue: "Socket"
                  },
                },
                MessageBody: JSON.stringify({ type: postData.cmd, alarmIds: alarms, gatewayId : `stage:ic-${serialNumber}`, email : email}),
                // MessageDeduplicationId: "TheWhistler",  // Required for FIFO queues
                // MessageGroupId: "Group1",  // Required for FIFO queues
                QueueUrl: "https://sqs.eu-west-1.amazonaws.com/314597539969/RemoteActions"
              };
              //dataToSend.push(params)
              console.log(`sqs.sendMessage: message: ${params}`);
              let data = await sqs.sendMessage(params).promise().catch((err) => {
                console.error(`sqs.sendMessage: Error message: ${err}`);
              });
              console.log(`sqs.sendMessage: message: ${data}`);
    
            } catch (error) {
              throw Error({ statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(error) });
            }


            // Ack SQS
            try {
              var params = {
                // Remove DelaySeconds parameter and value for FIFO queues
                DelaySeconds: 0,
                MessageAttributes: {
                  "Title": {
                    DataType: "String",
                    StringValue: "Socket"
                  },
                },
                MessageBody: JSON.stringify({ type: postData.cmd, alarmIds: groupedByGatewayAlarmIds[serialNumber], gatewayId : `stage:ic-${serialNumber}`, email : email}),
                // MessageDeduplicationId: "TheWhistler",  // Required for FIFO queues
                // MessageGroupId: "Group1",  // Required for FIFO queues
                QueueUrl: "https://sqs.eu-west-1.amazonaws.com/314597539969/RemoteActionPending"
              };
              //dataToSend.push(params)
              console.log(`sqs.sendMessage2: message: ${params}`);
              let data = await sqs.sendMessage(params).promise().catch((err) => {
                console.error(`sqs.sendMessage: Error message: ${err}`);
              });
              console.log(`sqs.sendMessage2: message: ${data}`);
    
            } catch (error) {
              throw Error({ statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(error) });
            } 
            
          }
          
      break;    
    default:
      await defaultCMD(event,connectionData,postData);
  }
  

  const sentSQSPDFReport = (cmd,url) => {
      // Ack SQS
      try {
        var params = {
          // Remove DelaySeconds parameter and value for FIFO queues
          DelaySeconds: 0,
          MessageAttributes: {
            "Title": {
              DataType: "String",
              StringValue: "Socket"
            },
          },
          MessageBody: JSON.stringify({ type: cmd, pdf : url}),
          // MessageDeduplicationId: "TheWhistler",  // Required for FIFO queues
          // MessageGroupId: "Group1",  // Required for FIFO queues
          QueueUrl: "https://sqs.eu-west-1.amazonaws.com/314597539969/PDF_Report"
        };
        //dataToSend.push(params)
        console.log(`sqs.sendMessage2: message: ${params}`);
        let data = await sqs.sendMessage(params).promise().catch((err) => {
          console.error(`sqs.sendMessage: Error message: ${err}`);
        });
        console.log(`sqs.sendMessage2: message: ${data}`);

      } catch (error) {
        throw Error({ statusCode: 500, body: 'Failed to add cmd to queue. ' + JSON.stringify(error) });
      } 
  }


  return { statusCode: 200, body: 'Data sent.' };
};