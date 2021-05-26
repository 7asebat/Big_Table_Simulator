const fs = require("fs");
var path = require('path');

let csvFileName = process.argv[2]
let jsonFileName = process.argv[3]
csv = fs.readFileSync(path.join(".","..","assets",csvFileName))

let rows = csv.toString().split("\r\n");
let keys=[]
let jsonObjects = []
rows.forEach((row,index)=>{
    if(index==0){
        keys = row.split(',')
    }
    else{
        let vals = row.split(',')
        let entry = {}
        keys.forEach((key,index)=>{
            entry[key] = vals[index]
        })
        jsonObjects.push(entry)
    }
});

fs.writeFileSync(path.join(".","..","assets",jsonFileName),JSON.stringify(jsonObjects));