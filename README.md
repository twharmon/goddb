# GoDDB
An opinionated package to simplify working with a single table in AWS DynamoDB.

## Rules
- Values associated with items in a table must be structs.
- Your primary key must be a composite primary key with partition `PK` and sort `SK` (both strings in table).
- Your *shared* global secondary indexes (those used by multiple Go structs) must be composite with partition suffix `PK` and sort suffix `SK` (both strings in table).
- Your *unshared* global secondary indexes (those used by only one Go struct) must be simple with partition `<StructName>GSI` (string in table). 
- You must specify primary key and global secondary indexes on your struct fields with the `goddb` tag.
- Struct fields tagged with primary key or global secondary indexes must one of the following types:
    - string
    - int
    - int8
    - uint
    - uint8
    - int16
    - uint16
    - int32
    - uint32
    - int64
    - uint64
    - float32
    - float64
- The following environment vairables must be set:
    - AWS_REGION
    - GODDB_TABLE_NAME

## Examples
```go
// define user
type User struct {
  ID   string `goddb:"PK,SK,UserGSI"`
  Name string
}

// add some users to table
goddb.Put(&User{ID: "bob", Name: "Bob"}).Exec()
goddb.Put(&User{ID: "bill", Name: "Bill"}).Exec()

// query (DynamoDB scan) all users (requires global secondary index UserGSI)
users, _ := goddb.Query(&User{}).Exec()

// update user
goddb.Update(&User{ID: "bob"}).Set(&User{Name: "Robert"}).Exec()

// get user
user, _ := goddb.Get(&User{ID: "bob"}).Exec()

// delete user
goddb.Delete(&User{ID: "bob"}).Exec()

// define post
type Post struct {
  ID     string `goddb:"SK"`
  Author string `goddb:"PK"`
  Body   string
}

// add some posts to table
goddb.Put(&User{ID: "hello", Author: "bill", Body: "Hi!"}).Exec()
goddb.Put(&User{ID: "bye", Name: "bill", Body: "Bye!"}).Exec()

// query all of Bill's posts
posts, _ := goddb.Query(&Post{Author: "bill"}).Exec()

// paginate through Bill's posts
var offset string
posts, _ := goddb.Query(&Post{Author: "bill"}).Page(10, &offset).Exec()
// returns Bill's first 10 posts (sorted by id)

// Note:
// Exec() will mutate offset to indicate last item
// send new offset along with posts to client
// client should send offset back when asking for next page

posts, _ := goddb.Query(&Post{Author: "bill"}).Page(10, &offset).Exec()
// returns Bill's next 10 posts

// Note:
// if query reaches the last of posts, offset will be set back to empty string


// delete all of Bill's posts
goddb.DeleteAll(&Post{Author: "bill"}).Exec()
```
