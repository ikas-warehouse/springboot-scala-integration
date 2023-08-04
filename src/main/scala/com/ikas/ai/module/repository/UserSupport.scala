package com.ikas.ai.module.repository

import com.ikas.ai.module.enties.User
import org.springframework.data.repository.PagingAndSortingRepository

trait UserSupport extends PagingAndSortingRepository[User, Long] {

}
