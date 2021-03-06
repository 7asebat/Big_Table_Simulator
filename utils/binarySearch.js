const binarySearch = function (arr, x, start, end) {
       
    // Base Condition
    if (start > end) return {"data":null,"index":-1};
   
    // Find the middle index
    let mid=Math.floor((start + end)/2);
   
    // Compare mid with given key x
    if (arr[mid].user_id===x) return {"data":arr[mid],"index":mid};
          
    // If element at mid is greater than x,
    // search in the left half of mid
    if(arr[mid].user_id > x) 
        return binarySearch(arr, x, start, mid-1);
    else
  
        // If element at mid is smaller than x,
        // search in the right half of mid
        return binarySearch(arr, x, mid+1, end);
}

exports.binarySearch = binarySearch;