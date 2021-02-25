package bigData

case class Event(
                  actor: Actor,
                  created_at: String,
                  id: String,
                  org: String,
                  payload: Payload,
                  publicField: Boolean,
                  repo: String,
                  `type`: String) {

}
