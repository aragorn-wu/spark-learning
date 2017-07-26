package com.git.wuqf.demos.movie

object SqlConstant {

  object userSql {
    val genderStatistic = "select distinct gender,count(*) from user group by gender";
    val ageStatistic = "select distinct age,count(*) from user group by age";
  }

}
