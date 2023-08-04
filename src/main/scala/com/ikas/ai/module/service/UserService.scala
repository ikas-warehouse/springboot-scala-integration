package com.ikas.ai.module.service

import com.ikas.ai.module.enties.User
import org.springframework.data.domain.{Page, Pageable}

trait UserService {

  /**
    * save model to db
    */
  def save(model: User): User;

  /**
    * get all model
    */
  def getAll(page: Pageable): Page[User]

}
