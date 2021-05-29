const MAX_TABLET_SIZE = 1000;
const bigTable = require("./../assets/users.json");
const partitionData = require("./../utils/partitionData.js");
const generateMetadata = require("./../utils/generateMetadata.js");
const PORT = 3000;
const io = require("socket.io")(PORT);
let tablets = partitionData(bigTable,MAX_TABLET_SIZE);
let metadata = generateMetadata(tablets,MAX_TABLET_SIZE,bigTable.length);
const {binarySearch} = require("./../utils/binarySearch");

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
            tabletsIds = metadata[tabletConnections.length-1].tablets_range;
            start = Math.floor(tabletsIds[0]/MAX_TABLET_SIZE);
            end = Math.floor(tabletsIds[1]/MAX_TABLET_SIZE)+1; 
            console.log(start,end,tabletsIds)
            io.to(socket.id).emit("tablets",tablets.slice(start,end));
        }

        console.log({clientConnections,tabletConnections});
    });

    socket.on("disconnect",()=>{
        clientConnections=clientConnections.filter(connection=>connection.socket_id!==socket.id);
        tabletConnections=tabletConnections.filter(connection=>connection.socket_id!==socket.id);
        console.log({clientConnections,tabletConnections});
    });

    socket.on("periodic_update",(updatedData,deletedData)=>{
        updatedElements=[];
        deletedElements=[];
        updatedData.forEach(el=>{
            let{data,index} = binarySearch(bigTable,el.user_id,0,bigTable.length-1);
            if (index != -1){
                bigTable[index] = el;
                updatedElements.push(bigTable[index]);
            }
        });
        deletedData.forEach(el=>{
            let{data,index} = binarySearch(bigTable,el.user_id,0,bigTable.length-1);
            if (index != -1){
                deletedEl=bigTable.splice(index,1);
                deletedElements.push(deletedEl);
            }
        });
        socketName = tabletConnections[0].socket_id == socket.id ? "Tablet 1":"Tablet 2";
        console.log(`Server ${socketName} periodically updated elements: `,updatedElements);
        console.log(`Server ${socketName} periodically deleted elements: `,deletedElements);

    });
});












