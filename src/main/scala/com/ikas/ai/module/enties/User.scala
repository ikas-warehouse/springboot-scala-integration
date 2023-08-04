package com.ikas.ai.module.enties

import javax.persistence.{Entity, GeneratedValue, Id, Table}

@Entity
@Table(name = "user")
class User {

  @Id
  @GeneratedValue
  var id: Long = 0

  var name: String = null

}
