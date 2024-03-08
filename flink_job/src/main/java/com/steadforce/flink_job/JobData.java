package com.steadforce.flink_job;

public class JobData {
   private String id;
   private String data;

   public JobData() {
   }

   public JobData(String id, String data) {
       this.id = id;
       this.data = data;
   }

   public String getId() {
       return id;
   }

   public void setId(String id) {
       this.id = id;
   }

   public String getData() {
       return data;
   }

   public void setData(String data) {
       this.data = data;
   }
}