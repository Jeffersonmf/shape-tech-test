package com.shape.test.data.exceptions

class ParserException(errorDetails: String) extends Exception {

  throw new Exception(String.format("An Unexpected Error occurred on trying to load the data from logs.: %s",
    errorDetails))
}

final case class ExtractLogException(private val message: String = "An Unexpected Error occurred on trying to load the data from logs", private val cause: Throwable = None.orNull)
  extends Exception(message, cause)

