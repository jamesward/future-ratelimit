import akka.actor.ActorSystem
import org.scalatest.{AsyncWordSpec, MustMatchers}

import scala.concurrent.Future
import scala.concurrent.duration._

class FutureRateLimitSpec extends AsyncWordSpec with MustMatchers {

  implicit lazy val actorSystem: ActorSystem = ActorSystem()

  // will create no more tasks than the specified rate but tasks that take longer than the interval will result in more concurrent tasks than the rate
  def runWithCreateRateLimit[A, B](maxItemsPerInterval: Int, interval: FiniteDuration)(items: Traversable[A])(runner: A => Future[B]): Future[Traversable[B]] = {
    val (doNow, doLater) = items.splitAt(maxItemsPerInterval)
    val doNowFuture = Future.sequence(doNow.map(runner))
    if (doLater.isEmpty) {
      doNowFuture
    }
    else {
      val doLaterFuture = akka.pattern.after(interval, actorSystem.scheduler)(runWithCreateRateLimit(maxItemsPerInterval, interval)(doLater)(runner))
      for {
        doNowResult <- doNowFuture
        doLaterResult <- doLaterFuture
      } yield doNowResult ++ doLaterResult
    }
  }

  "runWithCreateRateLimit" must {
    "work when all tasks take less time than the interval" in {
      val items = 1 to 100

      def runner[A](a: A) = Future.successful(a)

      val startTime = System.nanoTime()

      runWithCreateRateLimit(10, 1.second)(items)(runner).map { result =>
        Duration.fromNanos(System.nanoTime() - startTime) must (be > 9.seconds and be < 10.seconds)
        result must equal (items)
      }
    }
    "work when all tasks take more time than the interval" in {
      val items = 1 to 10

      def runner[A](a: A) = akka.pattern.after(2.seconds, actorSystem.scheduler)(Future.successful(a))

      val startTime = System.nanoTime()

      runWithCreateRateLimit(2, 1.second)(items)(runner).map { result =>
        Duration.fromNanos(System.nanoTime() - startTime) must (be > 6.seconds and be < 7.seconds)
        result must equal (items)
      }
    }
    "work when there the number of items doesn't align to the rate" in {
      val items = 1 to 5

      def runner[A](a: A) = Future.successful(a)

      val startTime = System.nanoTime()

      runWithCreateRateLimit(2, 1.second)(items)(runner).map { result =>
        Duration.fromNanos(System.nanoTime() - startTime) must (be > 2.seconds and be < 3.seconds)
        result must equal (items)
      }
    }
  }

}
