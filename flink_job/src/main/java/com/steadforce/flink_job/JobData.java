package com.steadforce.flink_job;

public class JobData {
   private Long id;
   private String data;

   public JobData() {
   }

   public JobData(Long id, String data) {
       this.id = id;
       this.data = data;
   }

   public Long getId() {
       return id;
   }

   public void setId(Long id) {
       this.id = id;
   }

   public String getData() {
       return data;
   }

   public void setData(String data) {
       this.data = data;
   }
}