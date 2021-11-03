package com.shape.test.data.router

import colossus.core.ServerContext
import colossus.protocols.http.HttpMethod.Get
import colossus.protocols.http.{Http, RequestHandler}
import colossus.protocols.http.UrlParsing.{/, Root, on}
import colossus.service.Callback
import colossus.service.GenRequestHandler.PartialHandler
import com.shape.test.data.api.shapeTestAPIs

class HttpRouterHandler(context: ServerContext) extends RequestHandler(context) {

  override def handle: PartialHandler[Http] = {
    case request@Get on Root / "healthcheck" => {
      Callback.successful(request.ok("Welcome to Scala Akka - Shape Test is UP!!"))
    }
    case request @ Get on Root => {
      Callback.successful(request.ok(shapeTestAPIs.computeDataFailuresFromLake()._1))
    }
  }
}