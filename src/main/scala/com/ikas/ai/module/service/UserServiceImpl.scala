package com.ikas.ai.module.service

import com.ikas.ai.module.enties.User
import com.ikas.ai.module.repository.UserSupport
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.domain.{Page, Pageable}
import org.springframework.stereotype.Service

@Service(value = "userService")
class UserServiceImpl @Autowired()(
                                    val userSupport: UserSupport
                                  ) extends UserService {

  /**
    * save model to db
    */
  override def save(model: User): User = {
    return this.userSupport.save(model)
  }

  /**
    * get all model
    */
  override def getAll(page: Pageable): Page[User] = {
    return this.userSupport.findAll(page)
  }

}
