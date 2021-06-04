const MASTER_PORT = 3000;
const TABLET_PORT = process.argv[2];
const DATABASENAME = process.argv[3];
const DATABASE = "mongodb://127.0.0.1:27017/" + DATABASENAME;
let socket = require("socket.io-client")(`http://localhost:${MASTER_PORT}`);
const io = require("socket.io")(TABLET_PORT);
const { binarySearch } = require("./../utils/binarySearch");
const count2d = require("./../utils/countArr");
const { dropDB } = require("./../utils/dropDb");

const schema = require("./../models/tabletSchema");
let models = [];
//DB connection
const mongoose = require("mongoose");
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
//Holds data to be updated periodically
let updatedData = [];
let deletedData = [];
let dataCount = 0;

//Send an event to master server to update main table
setInterval(function () {
  if (updatedData.length || deletedData.length) {
    console.log("Current data count = ", dataCount);
    socket.emit("periodic_update", updatedData, deletedData, dataCount);
    updatedData = [];
    deletedData = [];
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

socket.on("partition", (data) => {
  console.log("Received partition data");
  tablets = data;
  dataCount = count2d(tablets);
  models.forEach(async (md) => {
    await mongoose.connection.db.dropCollection(`${md}`);
  });
  models = [];
  tablets.forEach((tb, index) => {
    var model = mongoose.model(`${index}`, schema);
    models.push(model);
    model.collection.insertMany(tb, (err, docs) => {
      if (err) {
        console.log(err);
      } else {
        console.log("Data inserted successfully");
      }
    });
  });
});

socket.on("data", (data) => {
  tablets = data;
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
});

const existsInUpdatedData = (id) => {
  found = updatedData.findIndex((el) => el.user_id == id) !== -1 ? true : false;
  return found;
};

const requestHandler = async (type, q) => {
  results = [];
  q.row_key.forEach(async (key) => {
    tabletModel = getTabletModel(key);
    console.log("MODEL ", tabletModel);

    if (tabletModel == -1)
      results.push(`row with user_id = ${key} wasn't found`);
    else {
      let result = await tabletModel.findOne({ user_id: key });

      if (!result) results.push(`row with user_id = ${key} wasn't found`);

      else {
        resultObj = result.toObject();
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

          case "DeleteRow":
            deletedEl = resultObj;
            await tabletModel.deleteOne({ user_id: key });
            results.push(`Entry with key = ${key} is deleted successfully`);
            deletedData.push(deletedEl);
            break;
        }
      }
    }
  });
  return results;
};
