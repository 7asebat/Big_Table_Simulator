
/*  metadata structure
    [{
        tablets_count: number,
        tablets_ids:[],
    }]
*/

module.exports = (tablets,MAX_TABLET_SIZE,usersLength)=>{
    const division = Math.floor(tablets.length/2);
    let ids_1=[], ids_2=[]
    for(let i=0;i<tablets.length;i++){
        if(i<division)
            ids_1.push(i);
        else
            ids_2.push(i);
    }
    return [{
        "tablets_count":division,
        "tablets_ids":[1,Math.floor(usersLength/2)]
    },
    {
        "tablets_count":tablets.length-division,
        "tablets_ids":[Math.floor(usersLength/2+1),usersLength]
    }]
}