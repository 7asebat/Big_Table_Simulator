
/*  metadata structure
    [{
        tablets_count: number,
        tablets_ids:[],
    }]
*/

module.exports = (tablets,MAX_TABLET_SIZE,usersLength)=>{
    const division = Math.floor(tablets.length/2);
    let ids_1=[], ids_2=[]
    return [{
        "tablets_count":division,
        "tablets_range":[1,division*MAX_TABLET_SIZE]
    },
    {
        "tablets_count":tablets.length-division,
        "tablets_range":[(division*MAX_TABLET_SIZE)+1,(tablets.length)* MAX_TABLET_SIZE]
    }]
}