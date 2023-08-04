package com.ikas.ai.module.controller

import com.ikas.ai.module.enties.User
import com.ikas.ai.module.service.UserService
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.data.domain.{Page, PageRequest}
import org.springframework.web.bind.annotation._

@RestController
@RequestMapping(value = Array("user"))
class UserController @Autowired()(
                                   val userService: UserService
                                 ) {

  @PostMapping(value = Array("save/{name}"))
  def save(@PathVariable name: String): Long = {
    val userModel = {
      new User()
    }
    userModel.name = name
    return this.userService.save(userModel).id
  }

  @GetMapping(value = Array("list"))
  def get(): Page[User] = this.userService.getAll(PageRequest.of(0, 10))

}
