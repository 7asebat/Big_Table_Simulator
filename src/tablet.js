const MASTER_PORT = 3000;
const TABLET_PORT = process.argv[2];
const DATABASENAME = process.argv[3];
let MAX_TABLET_SIZE = 200;
const DATABASE = "mongodb://127.0.0.1:27017/" + DATABASENAME;
let socket = require("socket.io-client")(`http://localhost:${MASTER_PORT}`);
const io = require("socket.io")(TABLET_PORT);
const count2d = require("./../utils/countArr");
var Mutex = require('async-mutex').Mutex;
const schema = require("./../models/tabletSchema");
const mongoose = require("mongoose");
const fs = require('fs');

let logFile = `./../logs/tablet${TABLET_PORT}Log.txt`;
fs.writeFileSync(logFile,"BEGIN LOGS\n");

let models = [];
//DB connection
const connectToDB = async () => {
  mongoose
    .connect(DATABASE, {
      useNewUrlParser: true,
      useCreateIndex: true,
      useFindAndModify: false,
      useUnifiedTopology: true,
    })
    .then(() => {
      console.log("Connection to database successful ✅");
    })
    .catch((e) => {
      console.log(e);
      console.error("Failed to connect to database, retrying in one second...");
      setTimeout(connectToDB, 1000);
    });
};

(async () => {
  await connectToDB();
})();

process.on("SIGINT", async () => {
  await mongoose.connection.db.dropDatabase();
  process.exit(0);
});

let tablets = [];
let lock = new Mutex();
//Holds data to be updated periodically
let updatedData = [];
let deletedData = [];
let addedData = [];
let dataCount = 0;

//Send an event to master server to update main table
setInterval(function () {
  if (updatedData.length || deletedData.length || addedData.length) {
    fs.appendFileSync(logFile,"Periodic update event to master\n");
    socket.emit("periodic_update", addedData,updatedData, deletedData, models.length);
    updatedData = [];
    deletedData = [];
    addedData = [];
  }
}, 60 * 1000); // 60 * 1000 milsec

const getTabletModel = (row_key) => {
  let tabletModel = -1;
  models.forEach((model) => {
    if (model.start <= row_key && model.end >= row_key) tabletModel = model;
  });
  return tabletModel.model;
};

socket.on("connect", () => {
  socket.send({ type: "tablet" });
});

socket.on("partition", async (data) => {
  console.log("Received partition data");
  fs.appendFileSync(logFile,"Received partition data\n");

  tablets = data;
  dataCount = count2d(tablets);
  for(let i=0;i<models.length;i++){
    await mongoose.connection.db.dropCollection(`${i}`);
  }
  models = [];
  tablets.forEach((tb, index) => {
    var model = mongoose.model(`${index}`, schema);
    models.push(model);
    model.collection.insertMany(tb, (err) => {
      if (err) {
        console.log(err);
      }
    });
  });
  fs.appendFileSync(logFile,"Data partitioned successfully\n");
  console.log("Data partitioned successfully");
});

socket.on("data", (data,TABLET_SIZE) => {
  tablets = data;
  MAX_TABLET_SIZE = TABLET_SIZE;
  dataCount = count2d(tablets);
  tablets.forEach((tb, index) => {
    var model = mongoose.model(`${index}`, schema);
    models.push({
      model: model,
      start: tb[0].user_id,
      end: tb[tb.length - 1].user_id,
    });
    model.collection.insertMany(tb, (err, docs) => {
      if (err) {
        console.log(err);
      }
    });
  });
  fs.appendFileSync(logFile,"Received initial data\n");
  console.log(models);
});

io.on("connection", (socket) => {
  console.log("Client connected ", socket.id);
  socket.on("read", async (q, cb) => {
    console.log(
      "Received read request from client with socket id = ",
      socket.id
    );
    console.log(q);
    results = await requestHandler("Read", q);
    cb(results);
  });

  socket.on("delete_cells", async (q, cb) => {
    console.log(
      "Received delete cells request from client with socket id = ",
      socket.id
    );
    console.log(q);
    results = await requestHandler("DeleteCells", q);
    cb(results);
  });

  socket.on("delete_row", async (q, cb) => {
    console.log(
      "Received delete row request from client with socket id = ",
      socket.id
    );
    console.log(q);
    results = await requestHandler("DeleteRow", q);
    cb(results);
  });

  socket.on("set", async (q, cb) => {
    console.log(
      "Received set request from client with socket id = ",
      socket.id
    );
    console.log(q);
    results = await requestHandler("Set", q);
    cb(results);
  });

  socket.on("add_row", async (q, cb) => {
    console.log(
      "Received add row request from client with socket id = ",
      socket.id
    );
    console.log(q);
    results = await requestHandler("AddRow", q);
    cb(results);
  });
});

const existsInUpdatedData = (id) => {
  found = updatedData.findIndex((el) => el.user_id == id) !== -1 ? true : false;
  return found;
};

const requestHandler = async (type, q) => {
  let results = [];
  for (const key of q.row_key){
    tabletModel = getTabletModel(key);

    if (tabletModel == -1)
      results.push(`row with user_id = ${key} wasn't found`);
    else {
      let result = {};
      if(q.type !== "AddRow"){
        result = await tabletModel.findOne({ user_id: key });
      }

      if (!result) results.push(`row with user_id = ${key} wasn't found`);

      else {
        resultObj = result;
        switch (type) {
          case "Set":
            Object.entries(q.columns_data).forEach(([key, value]) => {
              result[`${key}`] = value;
            });
            if (!existsInUpdatedData(result.user_id))
              updatedData.push(resultObj);
            results.push(resultObj);
            await result.save();
            break;

          case "DeleteCells":
            q.columns.forEach((column) => {
              result[`${column}`] = null;
            });
            if (!existsInUpdatedData(result.user_id))
              updatedData.push(resultObj);
            results.push(resultObj);
            await result.save();
            break;

          case "Read":
            results.push(resultObj);
            break;
          
          case "AddRow":
            let newDoc = {};
            Object.entries(q.columns_data).forEach(([key, value]) => {
              newDoc[`${key}`] = value;
            });

            await lock.runExclusive(async () => {
              let lastModel = models[models.length-1]["model"];
              let count = await lastModel.countDocuments({});
              let lastUser = await (lastModel.find({}).sort({_id:-1}).limit(1));
              console.log(lastUser);
              newDoc["user_id"]=lastUser[0].user_id + 1;
              dataCount+=1;
              if(count<MAX_TABLET_SIZE){
                  await lastModel.create(newDoc);
              }else{
                  let newModel = mongoose.model(`${models.length}`,schema);
                  models.push({model:newModel,start:newDoc["user_id"],end:newDoc["user_id"]+MAX_TABLET_SIZE-1});
                  await newModel.create(newDoc);
                  socket.emit("range_update", models.length,newDoc["user_id"]);
              }
              addedData.push(newDoc);
              results.push(newDoc);
              console.log(dataCount);
            });

          break;

          case "DeleteRow":
            deletedEl = resultObj;
            await tabletModel.deleteOne({ user_id: key });
            results.push(`Entry with key = ${key} is deleted successfully`);
            deletedData.push(deletedEl);
            dataCount -=1;
            break;
        }
      }
    }
  };
  fs.appendFileSync(logFile,`Received query\n ${JSON.stringify(q)}\n Result of query \n${JSON.stringify(results)}`);
  return results;
};
