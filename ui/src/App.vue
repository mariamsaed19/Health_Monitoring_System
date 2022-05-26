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
    time1:null,
    time2:null
  }),
  methods:{
    async onshow(){
      let temp = []
      this.date1 = this.formatDate(this.date1,this.time1)
      this.date2 = this.formatDate(this.date2,this.time2)
      // let startTime = performance.now()
      await axios.get('http://localhost:8085/', {
        params: {
          "from": this.date1,
          "to": this.date2,
        }
      }).then(function (response) {
        //this.items = response.data
        if(response.data != null){

          temp = response.data
        }
        console.log('response >> ' ,response.data)
        console.log('type >> ' , typeof (response.data))
      })

      this.items = []
      let temp2 = []
      temp.forEach(stats);
      function stats(item, i) {
        let stat = item.split(',')
        console.log(stat, i)
        // count += parseInt(stat[5])
        let stat_json = {
            'serviceName': stat[0],
            'cpu_peak_time':stat[2],
            'disk_peak_time':stat[3],
            'ram_peak_time':stat[4],
            'count':stat[5],
            'cpu':stat[6],
            'disk':stat[7],
            'ram':stat[8],
        }
        console.log(stat_json)
        temp2.push(stat_json)
        console.log("temp2 :", temp2)
      }
      this.items = temp2
      console.log("items : ", this.items)
      // console.log('>>>>>>>>>>>>>>>', new Date(parseInt('1650834430')).toISOString())
      // console.log('overall throughput : ', count/(endTime - startTime) ,'msg/msec')
    },
    onchange(d1){
      console.log("*****************")
      this.date1=d1[0]
      this.date2=d1[1]
      this.time1=d1[2]
      this.time2=d1[3]
      console.log('data check', this.formatDate(this.date1,this.time1),">>>>>>>>>", this.formatDate(this.date2,this.time2))

    },
    formatDate(d,t){
      let arr = d.split('-')
      let arr2 = t.split(':')
      return arr[0] + '_' +  arr[1] + '_' + arr[2]+'_'+arr2[0]+'_'+arr2[1]
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