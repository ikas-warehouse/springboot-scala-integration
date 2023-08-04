package com.ikas.ai

import org.springframework.boot.SpringApplication
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.domain.EntityScan
import org.springframework.context.annotation.ComponentScan
import org.springframework.data.jpa.repository.config.EnableJpaAuditing

@SpringBootApplication
//@ComponentScan(value = Array(
//  "com.ikas.ai"
//))
//@EntityScan(value = Array(
//  "com.ikas.ai.model"
//))
@EnableJpaAuditing
class SpringBootScalaIntegration

object SpringBootScalaIntegration extends App {

  SpringApplication.run(classOf[SpringBootScalaIntegration])

}
