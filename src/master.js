const MAX_TABLET_SIZE = 1000;
const bigTable = require("./../assets/users.json");
const partitionData = require("./../utils/partitionData.js");
const generateMetadata = require("./../utils/generateMetadata.js");
const { socket } = require("zeromq");
const PORT = 3000;
const io = require("socket.io")(PORT);
let tablets = partitionData(bigTable,MAX_TABLET_SIZE);
let metadata = generateMetadata(tablets);

let clientConnections = []
let tabletConnections = []

//Handling socket connections between services
io.on("connection", socket => {
    socket.on("message",data=>{
        if (data.type == "client"){
            clientConnections.push({"socket_id":socket.id,"client_number":clientConnections.length+1});
            socket.join("client");
            io.to(socket.id).emit("metadata",metadata);
        }
        else if(data.type == "tablet"){
            tabletConnections.push({"socket_id":socket.id,"tablet_number":tabletConnections.length+1});
            tabletsIds = metadata[tabletConnections.length-1].tablets_ids;
            io.to(socket.id).emit("tablets",tablets.slice(tabletsIds[0],tabletsIds[tabletsIds.length-1]+1));
        }

        console.log({clientConnections,tabletConnections});
    });

    socket.on("disconnect",()=>{
        clientConnections=clientConnections.filter(connection=>connection.socket_id!==socket.id);
        tabletConnections=tabletConnections.filter(connection=>connection.socket_id!==socket.id);
        console.log({clientConnections,tabletConnections});
    })
});












