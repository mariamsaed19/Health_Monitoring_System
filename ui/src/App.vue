<template>
  <v-app>
    <v-container>
    <div class="headers">
    
    <h1 class="primary--text">
      Health Monitor Analyzer 
      <img src = "./assets/analysis.svg" alt="My Happy SVG"/>
      </h1>
    </div>
    <Datepicker
      @change="onchange($event)"
     />
    <v-btn
      rounded
      color="primary"
      dark
      sm="6" 
      @click="onshow"
    >
      Show statistics
    </v-btn>
    <Table :items="items"/>
    </v-container>
  </v-app> 
</template>

<script>
import Datepicker from './components/Datepicker.vue';
import Table from './components/Table.vue'
export default {
  name: 'App',

  components :{
    Datepicker ,
    Table


  },
  data: () => ({
    items: [],
    date1: (new Date(Date.now() - (new Date()).getTimezoneOffset() * 60000)).toISOString().substr(0, 10),
    date2: (new Date(Date.now() - (new Date()).getTimezoneOffset() * 60000)).toISOString().substr(0, 10),
  }),
  methods:{
    onshow(){
      console.log("hello?")
      //TODO: here axios request
      console.log(this.date1)
      console.log(this.date2)

    },
    onchange(d1){
        console.log("*****************")
        this.date1=d1[0]
        this.date2=d1[1]

    },
  }
};
</script>
<style >
  .headers{
    display: inline;
  }
  img{
    width: 50px;
    height: 50px;
    margin-top:20px ;
    display: inline;
  }
</style>