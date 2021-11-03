package com.shape.test.data

import akka.actor.ActorSystem
import colossus.core.{IOSystem, InitContext}
import colossus.protocols.http.{HttpServer, Initializer}
import com.shape.test.data.core.parserEngine._
import com.shape.test.data.router.HttpRouterHandler
import com.typesafe.scalalogging.LazyLogging

object dataEngineerShapeTestApp extends App with LazyLogging {

  /**
   * Example uses the AKKA "Actor Model" / Event Sourcing Clustered
   */
  override def main(args: Array[String]): Unit = {
    print("Loading the Application...")

    //Load the entire data and create the data lake.
    initializeDataLake()

      implicit val actorSystem = ActorSystem()
      implicit val ioSystem = IOSystem()

      HttpServer.start("ScalaAkka", 9000){ context => new AkkaInitializer(context) }
    }

    class AkkaInitializer(context: InitContext) extends Initializer(context) {
      override def onConnect = context => new HttpRouterHandler(context)
    }
}
