package com.steadforce.flink_job;

public class JobData {
   private long id;
   private String data;

   public JobData() {
   }

   public JobData(long id, String data) {
       this.id = id;
       this.data = data;
   }

   public long getId() {
       return id;
   }

   public void setId(long id) {
       this.id = id;
   }

   public String getData() {
       return data;
   }

   public void setData(String data) {
       this.data = data;
   }
}