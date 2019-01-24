package scala.ca.uwaterloo.SIGIR.task

abstract class Task {
  def process(content: String): Any
}