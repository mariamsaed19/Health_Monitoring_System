<template>
<v-container>
<v-row>
<v-col
    cols="12"
    sm="6"
    md="4"
    >
      <v-menu
        v-model="menu1"
        :close-on-content-click="false"
        :nudge-right="40"
        transition="scale-transition"
        offset-y
        min-width="auto"
      >
        <template v-slot:activator="{ on, attrs }">
          <v-text-field
            v-model="date1"
            @change="onupdate"
            label="Choose starting date"
            prepend-icon="mdi-calendar"
            readonly
            v-bind="attrs"
            v-on="on"
          ></v-text-field>
        </template>
        <v-date-picker
          v-model="date1"
          @input="menu1 = false"
          @change="onupdate"
        ></v-date-picker>
      </v-menu>
    </v-col>

    <v-col
    cols="12"
    sm="6"
    md="4"
    >
      <v-menu
        v-model="menu2"
        :close-on-content-click="false"
        :nudge-right="40"
        transition="scale-transition"
        offset-y
        min-width="auto"
      >
        <template v-slot:activator="{ on, attrs }">
          <v-text-field
            v-model="date2"
            label="Choose ending date"
            prepend-icon="mdi-calendar"
            readonly
            v-bind="attrs"
            v-on="on"
          ></v-text-field>
        </template>
        <v-date-picker
          v-model="date2"
          @input="menu2 = false"
          @change="onupdate"
        ></v-date-picker>
      </v-menu>
    </v-col>

</v-row>
</v-container>
</template>

<script>
  export default {
    name: 'date-picker',

    data: () => ({
    date1: (new Date(Date.now() - (new Date()).getTimezoneOffset() * 60000)).toISOString().substr(0, 10),
    menu1: false,
    date2: (new Date(Date.now() - (new Date()).getTimezoneOffset() * 60000)).toISOString().substr(0, 10),
    menu2: false,
    }),
    methods:{
      onupdate(){
        this.$emit('change',[this.date1,this.date2])
      }
    }
  }
</script>
