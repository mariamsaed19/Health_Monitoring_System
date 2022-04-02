<template>
  <v-app>
    <v-container>
    <Datepicker/>
    <v-btn
      rounded
      color="primary"
      dark
      sm="6"
    >
      Show statistics
    </v-btn>
    <Table/>
    </v-container>
  </v-app> 
</template>

<script>
import Datepicker from './components/Datepicker.vue';
//import Card from './components/Card.vue';
import Table from './components/Table.vue'
export default {
  name: 'App',

  components :{
    Datepicker ,
    Table


  },
  data: () => ({
    //
  }),
};
</script>
