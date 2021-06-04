const mongoose = require('mongoose');
const tabletSchema = new mongoose.Schema(
  {
      user_id:Number,
      user_screen_name:String,
      indegree:Number,
      outdegree:Number,
      bad_user_id:String
  }
);

module.exports = tabletSchema;
