package com.git.wuqf.demos.movie

object SqlConstant {
  val genderStatistic="select distinct gender,count(*) from user group by gender";
  val ageStatistic="select distinct age,count(*) from user group by age";
}
