package com.radiantblue.piazza

sealed trait TaskState {
  def id: Long
}
final case class Submitted(id: Long) extends TaskState
final case class Assigned(id: Long, node: String) extends TaskState
final case class InProgress(id: Long, node: String, progress: Int) extends TaskState
final case class Failed(id: Long, reason: String) extends TaskState
final case class Aborted(id: Long) extends TaskState
final case class Succeeded(id: Long, payload: String) extends TaskState

sealed trait TaskMessage
object Submit extends TaskMessage
final case class Check(id: Long) extends TaskMessage
final case class Abort(id: Long) extends TaskMessage
final case class Assign(id: Long, node: String) extends TaskMessage
final case class Progress(id: Long, node: String, progress: Int) extends TaskMessage
final case class AbortSuccess(id: Long) extends TaskMessage
final case class Fail(id: Long) extends TaskMessage

class MemoryTaskTracker {
  private var state: Map[Long, TaskState] = Map.empty
  private var nextId: Long = 0
  def send(message: TaskMessage): Option[TaskState] = synchronized {
    message match {
      case Submit =>
        nextId = nextId + 1
        state += (nextId -> Submitted(nextId))
        Some(Submitted(nextId))
      case Check(id) => state.get(id)
      case Abort(id) => 
        state.get(id) match {
          case Some(Failed(_, _) | Succeeded(_, _) | Aborted(_)) => None
          case None => None
          case Some(_) =>
            state += (id -> Aborted(id))
            Some(Aborted(id))
        }
      case AbortSuccess(id) =>
        state.get(id) match {
          case Some(Failed(_, _) | Succeeded(_, _) | Aborted(_)) => None
          case None => None
          case Some(_) =>
            state += (id -> Aborted(id))
            Some(Aborted(id))
        }
      case Fail(id) =>
        state.get(id) match {
          case Some(Failed(_, _) | Succeeded(_, _) | Aborted(_)) => None
          case None => None
          case Some(_) =>
            state += (id -> Failed(id, "No reason given"))
            Some(Failed(id, "No reason given"))
        }
      case Assign(id, node) =>
        state.get(id) match {
          case Some(Submitted(_)) => 
            state += (id -> Assigned(id, node))
            Some(Assigned(id, node))
          case Some(_) | None =>
            None
        }
      case Progress(id, node, progress) =>
        state.get(id) match {
          case Some(Assigned(_, _) | InProgress(_, _, _)) =>
            state += (id -> InProgress(id, node, progress))
            Some(InProgress(id, node, progress))
          case Some(_) | None => 
            None
        }
    }
  }
  /*
  CREATE TYPE job_status AS ENUM ('submitted', 'aborted', 'assigned', 'failed', 'aborting', 'progressing', 'successful');

  CREATE TABLE jobs (
    id SERIAL PRIMARY KEY,
    "user" BIGINT NOT NULL,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    status job_status NOT NULL,
    payload_uri VARCHAR,
    failure_reason VARCHAR,
    progress INTEGER,
    CHECK (CASE WHEN status = 'successful' THEN payload_uri IS NOT NULL ELSE payload_uri IS NULL END),
    CHECK (CASE WHEN status = 'failed' THEN failure_reason IS NOT NULL ELSE failure_reason IS NULL END),
    CHECK (CASE WHEN status = 'progressing' THEN progress IS NOT NULL ELSE progress IS NULL END)
  );
  */
}
