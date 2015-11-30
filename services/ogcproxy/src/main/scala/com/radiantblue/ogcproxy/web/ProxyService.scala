package com.radiantblue.ogcproxy.web

import com.radiantblue.piazza.postgres._

import scala.concurrent.ExecutionContext
import scala.concurrent.Future

import akka.actor.Actor
import akka.actor.ActorSystem
import akka.io.IO
import spray.can.Http
import spray.routing._
import spray.http._, MediaTypes._

class ProxyServiceActor extends Actor with ProxyService {
  implicit val system = context.system
  def futureContext = context.dispatcher
  def actorRefFactory = context
  def receive = runRoute(proxyRoute)
}

trait Proxy {
  private def proxyRequest(updateRequest: RequestContext => HttpRequest)(implicit system: ActorSystem): Route =
    ctx => IO(Http)(system) tell (updateRequest(ctx), ctx.responder)

  private def stripHostHeader(headers: List[HttpHeader]) =
    headers filterNot (_ is HttpHeaders.Host.lowercaseName)

  private val updateUriUnmatchedPath = 
    (ctx: RequestContext, uri: Uri) => uri.withPath(uri.path ++ ctx.unmatchedPath)

  private val updateUriAuthority = 
    (ctx: RequestContext, uri: Uri) => ctx.request.uri.withAuthority(uri.authority)

  def updateRequest(uri: Uri, updateUri: (RequestContext, Uri) => Uri): RequestContext => HttpRequest =
    ctx =>
      ctx.request.copy(
        uri = updateUri(ctx, uri),
        headers = stripHostHeader(ctx.request.headers))

  def proxyToUnmatchedPath(uri: Uri)(implicit system: ActorSystem): Route =
    proxyRequest(updateRequest(uri, updateUriUnmatchedPath))

  def proxyToAuthority(uri: Uri)(implicit system: ActorSystem): Route =
    proxyRequest(updateRequest(uri, updateUriAuthority))
}

sealed trait ServiceType
case object WCS extends ServiceType
case object WFS extends ServiceType
case object WMS extends ServiceType

trait ProxyService extends HttpService with Proxy {
  implicit def futureContext: ExecutionContext
  implicit def system: ActorSystem

  def caseFoldedParameter(name: String): Directive1[String] = 
    parameterSeq.flatMap { params =>
      val lastMatch = 
        params.foldLeft(None: Option[String]) { (acc, el) =>
          if (el._1 equalsIgnoreCase name)
            Some(el._2)
          else
            acc
        }

      lastMatch.fold(reject: Directive1[String])(provide(_))
    }

  def ogcServiceParam: Directive1[ServiceType] =
    caseFoldedParameter("service").flatMap { svc =>
      if (svc equalsIgnoreCase "WCS")      provide(WCS: ServiceType)
      else if (svc equalsIgnoreCase "WFS") provide(WFS: ServiceType)
      else if (svc equalsIgnoreCase "WMS") provide(WMS: ServiceType)
      else                                 reject
    }

  def ogcService: Directive1[ServiceType] =
    (pathPrefix("wms") & (ogcServiceParam | provide(WMS: ServiceType))) |
    (pathPrefix("wfs") & (ogcServiceParam | provide(WFS: ServiceType))) |
    (pathPrefix("wcs") & (ogcServiceParam | provide(WCS: ServiceType))) |
    (pathPrefix("ows") & ogcServiceParam)

  def datasetId: Directive1[String] =
    ogcService
      .flatMap {
        case WCS => caseFoldedParameter("COVERAGEID")
        case WFS => caseFoldedParameter("TYPENAME").filter(! _.contains(',')) | reject(MalformedQueryParamRejection("FEATURETYPES", "Cannot proxy requests for multiple featuretypes (no comma allowed!)"))
        case WMS => caseFoldedParameter("LAYERS").filter(! _.contains(',')) | reject(MalformedQueryParamRejection("LAYERS", "Cannot proxy requests for multiple layers (no comma allowed!)"))
      }

  val lookup: String => Future[Uri] =
    id => Future {
      val conn = Postgres("piazza.metadata.postgres").connect()
      try {
        val servers = (new com.radiantblue.deployer.PostgresTrack(conn)).deployments(id)
        val srv = servers(1 % servers.size)
        s"http://${srv.getHost}:${srv.getPort}" : Uri
      } finally conn.close()
    }

  def proxyRoute: Route = 
    logRequestResponse("test") { 
      pathPrefix("geoserver") { 
        getFromResourceDirectory("com/radiantblue/ogcproxy/web") ~
        datasetId { layer =>
          val id = layer.replaceFirst("^piazza:", "")
          import scala.util.{ Success, Failure }
          onComplete(lookup(id)) {
            case Success(url) => proxyToAuthority(url)
            case Failure(ex) => complete(StatusCodes.NotFound, s"No dataset with locator $id is deployed: $ex")
          }
        } ~
        ogcService {
          case WMS => getFromResource("com/radiantblue/ogcproxy/wms-capabilities.xml")
          case WFS => complete("WFS!")
          case WCS => complete("WCS")
        }
      }
    }
}
