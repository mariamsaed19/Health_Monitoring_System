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
import axios from 'axios';

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
    async onshow(){
      let temp = []
      this.date1 = this.formatDate(this.date1)
      this.date2 = this.formatDate(this.date2)
      let startTime = performance.now()
      await axios.get('http://localhost:8085/', {
        params: {
          "from": this.date1,
          "to": this.date2,
        }
      }).then(function (response) {
            //this.items = response.data
            if(response.data != null){
              temp = response.data.map(JSON.parse)
            }
            // console.log('response >> ' , this.items)
            // console.log('type >> ' , typeof (response.data))
      })
      // console.log("out >>> " , typeof (this.items))
      // console.log(this.items)
      let endTime = performance.now()
      this.items = temp
      console.log('response : ' , this.items)
      console.log('latency : ', endTime - startTime, 'msec')
      let count = 0
      this.items.forEach(stats);
      function stats(item, i) {
          console.log(item, i)
          count += parseInt(item['count'])
          // console.log('item >> ', item['cpu_peak_time'], typeof (item['cpu_peak_time']))
          // item['cpu_peak_time'] = new Date(parseInt(item['cpu_peak_time']))
          // item['disk_peak_time'] = new Date(parseInt(item['disk_peak_time']))
          // item['ram_peak_time'] = new Date(parseInt(item['ram_peak_time']))
      }
      // console.log('>>>>>>>>>>>>>>>', new Date(parseInt('1650834430')).toISOString())
      console.log('overall throughput : ', count/(endTime - startTime) ,'msg/msec')
    },
    onchange(d1){
        console.log("*****************")
        this.date1=d1[0]
        this.date2=d1[1]
        console.log('data check', this.formatDate(this.date1), this.formatDate(this.date2))
    },
    formatDate(d){
        let arr = d.split('-')
        return arr[2] + '_' +  arr[1] + '_' + arr[0]
    }
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