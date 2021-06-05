const MASTER_PORT = 3000;
const TABLET1_PORT = 4000;
const TABLET2_PORT = 5000;

let masterSocket = require("socket.io-client")(
  `http://localhost:${MASTER_PORT}`
);
let tablet1Socket = require("socket.io-client")(
  `http://localhost:${TABLET1_PORT}`
);
let tablet2Socket = require("socket.io-client")(
  `http://localhost:${TABLET2_PORT}`
);

let queries = require("./../cases/test2.json");
const fs = require('fs');
let metadata = [];

let logFile = "./../logs/clientLogs.txt";

fs.writeFileSync(logFile,"BEGIN LOGS\n");


const init = async () => {
  const connections = [masterSocket, tablet1Socket, tablet2Socket];
  const promises = connections.map((connection) => {
    return new Promise((resolve, reject) => {
      connection.on("connect", () => {
        if (connection == masterSocket) masterSocket.send({ type: "client" });
        resolve();
      });
    });
  });

  return Promise.all(promises);
};

const targetServers = (keys) => {
  tablet1Keys = [];
  tablet2Keys = [];
  keys.forEach((key) => {
    metadata.forEach((entry, index) => {
      if (key >= entry.tablets_range[0] && key <= entry.tablets_range[1])
        index + 1 == 1 ? tablet1Keys.push(key) : tablet2Keys.push(key);
    });
  });

  return [tablet1Keys, tablet2Keys];
};

(async () => {
  await init();
  fs.appendFileSync(logFile,`Connected to master and the 2 tablet servers\n`);
  
  masterSocket.on("partition", (data) => {
    console.log("Received new metadata",data);
    fs.appendFileSync(logFile,`Received new metadata\n ${JSON.stringify(data)}\n`);
    metadata = data;
  });

  queries.forEach((query) => {
    switch (query.type) {
      case "Set":
        //Handle set queries
        // fs.appendFileSync(logFile,`Received Set Query\n ${JSON.stringify(query)}\n`);
        handleSetRequest(query);
        break;

      case "DeleteRow":
        //Handle Delete row queries
        // fs.appendFileSync(logFile,`Received Delete Row Query\n ${JSON.stringify(query)}\n`);
        handleDeleteRowRequest(query);
        break;

      case "DeleteCells":
        //Handle Delete cells queries
        // fs.appendFileSync(logFile,`Received Delete Cells Query\n ${JSON.stringify(query)}\n`);
        handleDeleteCellsRequest(query);
        break;

      case "AddRow":
        //Handle Add queries
        // fs.appendFileSync(logFile,`Received Add Row Query\n ${JSON.stringify(query)}\n`);
        handleAddRequest(query);
        break;

      case "Read":
        //Handle Read queries
        // fs.appendFileSync(logFile,`Received Read Query\n ${JSON.stringify(query)}\n`);
        handleReadRequest(query);
        break;
    }
  });
})();

masterSocket.on("metadata", (data) => {
  fs.appendFileSync(logFile,`Received metadata\n ${JSON.stringify(data)}\n`);
  metadata = data;
  console.log("Received metadata from master\n", metadata);
});

const handleReadRequest = (query) => {
  //Send each query to it's target server
  promises = globalHandler("read", query);
  Promise.all(promises);
};

const handleDeleteCellsRequest = (query) => {
  //Send each query to it's target server
  promises = globalHandler("delete_cells", query);
  Promise.all(promises);
};

const handleSetRequest = (query) => {
  //Send each query to it's target server
  promises = globalHandler("set", query);
  Promise.all(promises);
};

const handleAddRequest = (query) => {
  //Send each query to it's target server
  promises = globalHandler("add_row", query);
  Promise.all(promises);
};

const handleDeleteRowRequest = (query) => {
  //Send each query to it's target server
  promises = globalHandler("delete_row", query);
  Promise.all(promises);
};

const initQuery = (query) => {
  tablet1Queries = {};
  tablet2Queries = {};
  if (query.type == "AddRow") {
    let tempQuery = Object.assign({}, query);
    tempQuery.row_key = [query.row_key];
    Object.assign(tablet2Queries, tempQuery);
  } else {
    tabletsKeys =
      query.type == "Read" || query.type == "DeleteRow"
        ? targetServers(query.row_key)
        : targetServers([query.row_key]);

    //Separate queries for each tablet
    tabletsKeys.forEach((tabletKeys, index) => {
      if (tabletKeys.length != 0) {
        tempQuery = Object.assign({}, query);
        tempQuery.row_key = tabletKeys;
        index == 0
          ? Object.assign(tablet1Queries, tempQuery)
          : Object.assign(tablet2Queries, tempQuery);
      }
    });
  }
  return [tablet1Queries, tablet2Queries];
};

const globalHandler = (type, query) => {
  // console.log("Sending query of type = ", type, "Query is: ", query);
  serverQueries = initQuery(query);
  promises = [];
  serverQueries.forEach((q, index) => {
    if (!isEmptyObject(q)) {
      const tabletSocket = index + 1 == 1 ? tablet1Socket : tablet2Socket;
      promises.push(
        new Promise((resolve) => {
          tabletSocket.emit(`${type}`, q, (res) => {
            message =
              index + 1 == 1
                ? "Result from tablet server 1"
                : "Result from tablet server 2";
            // console.log(message, res);
            fs.appendFileSync(logFile,`Sending Query\n ${JSON.stringify(q)}\n`);
            fs.appendFileSync(logFile,`${message}\n ${JSON.stringify(res)}\n`);
            resolve(res);
          });
        })
      );
    }
  });
  return promises;
};

function isEmptyObject(obj) {
  return !Object.keys(obj).length;
}
