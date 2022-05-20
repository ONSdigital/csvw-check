package csvwcheck.traits

import com.typesafe.scalalogging.Logger

object LoggerExtensions {
  implicit class LogDebugException[T](logger: Logger) {

    /**
      * Extension method to support automatically printing message & stack trace when debugging exceptions.
      * @param error - The exception which is to be logged at the debug level.
      */
    def debug(error: Throwable): Unit = {
      logger.debug(error.getMessage)
      logger.debug(error.getStackTrace.mkString("\n"))
    }
  }

}
