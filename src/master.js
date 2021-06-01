const MAX_TABLET_SIZE = 1000;
const bigTable = require("./../assets/users.json");
const partitionData = require("./../utils/partitionData.js");
const generateMetadata = require("./../utils/generateMetadata.js");
const PORT = 3000;
const io = require("socket.io")(PORT);
let tablets = partitionData(bigTable,MAX_TABLET_SIZE);
let metadata = generateMetadata(tablets,MAX_TABLET_SIZE,bigTable.length);
const {binarySearch} = require("./../utils/binarySearch");

let clientConnections = [];
let tabletConnections = [];

//Handling socket connections between services
io.on("connection", socket => {
    socket.on("message",data=>{
        if (data.type == "client"){
            clientConnections.push({"socket_id":socket.id,"client_number":clientConnections.length+1});
            socket.join("client");
            io.to(socket.id).emit("metadata",metadata);
        }
        else if(data.type == "tablet"){
            console.log(metadata);
            tabletsIds = metadata[tabletConnections.length].tablets_range;
            console.log(tabletsIds);
            tabletConnections.push({"socket_id":socket.id,"tablet_number":tabletConnections.length+1,"data_count":tabletsIds[1]-tabletsIds[0]+1});
            start = Math.floor(tabletsIds[0]/MAX_TABLET_SIZE);
            end = Math.floor(tabletsIds[1]/MAX_TABLET_SIZE)+1; 
            io.to(socket.id).emit("data",tablets.slice(start,end));
        }

        console.log({clientConnections,tabletConnections});
    });

    socket.on("disconnect",()=>{
        clientConnections=clientConnections.filter(connection=>connection.socket_id!==socket.id);
        tabletConnections=tabletConnections.filter(connection=>connection.socket_id!==socket.id);
        console.log({clientConnections,tabletConnections});
    });

    socket.on("periodic_update",(updatedData,deletedData,dataCount)=>{
        updatedElements=[];
        deletedElements=[];
        tabletId = tabletConnections[0].socket_id == socket.id ? 1:2;
        tabletConnections[tabletId-1].data_count = dataCount;
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
        console.log(`Server ${tabletId} periodically updated elements: `,updatedElements);
        console.log(`Server ${tabletId} periodically deleted elements: `,deletedElements);

        t1DataCount = tabletConnections[0].data_count;
        t2DataCount = tabletConnections[1].data_count;
        if(checkAndReassign(t1DataCount,t2DataCount)){
            console.log("Reassigning");
            handleRepartition();
        }
    });

    //Checks partitioning
    const checkAndReassign = (t1DataCount,t2DataCount)=>{
        const reassigningFactor = 1/3 * tablets.length;
        if(Math.abs(t1DataCount-t2DataCount) >= reassigningFactor * MAX_TABLET_SIZE){
            tablets = partitionData(bigTable,MAX_TABLET_SIZE);
            metadata = generateMetadata(tablets,MAX_TABLET_SIZE,bigTable.length);
            return true;
        }
        return false;
    };
    
    //Sends data and metadata to tablets and clients
    const handleRepartition = ()=>{
        tabletConnections.forEach((connection,index)=>{
            socketId = connection.socket_id;
            tabletsIds = metadata[index].tablets_range;
            start = Math.floor(tabletsIds[0]/MAX_TABLET_SIZE);
            end = Math.floor(tabletsIds[1]/MAX_TABLET_SIZE)+1;
            io.to(socketId).emit("partition",tablets.slice(start,end));
        });
        io.to("client").emit("partition",metadata);
    }
});












