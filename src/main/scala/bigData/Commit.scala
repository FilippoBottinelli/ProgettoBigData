package bigData

case class Commit(
                   author: String,
                   distinct: Boolean,
                   message: String,
                   sha: String,
                   url: String
                 ){

}
