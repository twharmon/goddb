package goddb_test

import (
	"goddb"
	"testing"
	"time"
)

func TestBasic(t *testing.T) {
	type User struct {
		ID   string `goddb:"PK,SK,UserGSI"`
		Name string
	}

	type Post struct {
		ID       string `goddb:"SK,GSI1SK"`
		Author   string `goddb:"PK"`
		Body     string
		Category string `goddb:"GSI1PK"`
	}

	t.Run("put user", func(t *testing.T) {
		assertEq(t, goddb.Put(&User{ID: "abc", Name: "Jon Doe"}).Exec(), nil)
	})

	t.Run("get user", func(t *testing.T) {
		user, err := goddb.Get(&User{ID: "abc"}).Exec()
		assertEq(t, err, nil)
		assertEq(t, user.ID, "abc")
		assertEq(t, user.Name, "Jon Doe")
	})

	t.Run("put two valid posts", func(t *testing.T) {
		assertEq(t, goddb.Put(&Post{Author: "abc", ID: "abc", Body: "Foo bar", Category: "foo"}).Exec(), nil)
		assertEq(t, goddb.Put(&Post{Author: "abc", ID: "def", Body: "Baz bat", Category: "baz"}).Exec(), nil)
	})

	t.Run("put invalid valid posts", func(t *testing.T) {
		type Invalid struct {
			ID  string `goddb:"PK,SK"`
			Foo string `goddb:"SK"`
		}
		assertNe(t, goddb.Put(&User{}).Exec(), nil)
		assertNe(t, goddb.Put(&Post{}).Exec(), nil)
		assertNe(t, goddb.Put(&Post{Author: "abc"}).Exec(), nil)
		assertNe(t, goddb.Put(&Post{ID: "abc"}).Exec(), nil)
		assertNe(t, goddb.Put(&Invalid{}).Exec(), nil)
		assertNe(t, goddb.Put(&Invalid{ID: "abc"}).Exec(), nil)
		assertNe(t, goddb.Put(&Invalid{Foo: "abc"}).Exec(), nil)
		assertNe(t, goddb.Put(&Invalid{ID: "abc", Foo: "foo"}).Exec(), nil)
	})

	t.Run("query all on primary key", func(t *testing.T) {
		posts, err := goddb.Query(&Post{Author: "abc"}).Exec()
		assertEq(t, err, nil)
		assertEq(t, len(posts), 2)
		for _, post := range posts {
			assertEq(t, post.Author, "abc")
			assertEq(t, len(post.ID), 3)
			assertEq(t, len(post.Body), 7)
			assertEq(t, len(post.Category), 3)
		}
	})

	t.Run("query with pagination", func(t *testing.T) {
		var offset string
		posts, err := goddb.Query(&Post{Author: "abc"}).Page(1, &offset).Exec()
		assertEq(t, err, nil)
		assertEq(t, len(posts), 1)
		assertEq(t, posts[0].ID, "abc")
		posts, err = goddb.Query(&Post{Author: "abc"}).Page(1, &offset).Exec()
		assertEq(t, err, nil)
		assertEq(t, len(posts), 1)
		assertEq(t, posts[0].ID, "def")
		posts, err = goddb.Query(&Post{Author: "abc"}).Page(1, &offset).Exec()
		assertEq(t, err, nil)
		assertEq(t, len(posts), 0)
		assertEq(t, offset, "")
	})

	t.Run("query begins with on primary key", func(t *testing.T) {
		posts, err := goddb.Query(&Post{Author: "abc"}).
			BeginsWith(&Post{ID: "a"}).
			Exec()
		assertEq(t, err, nil)
		assertEq(t, len(posts), 1)
		for _, post := range posts {
			assertEq(t, post.Author, "abc")
			assertEq(t, post.ID, "abc")
			assertEq(t, post.Body, "Foo bar")
			assertEq(t, post.Category, "foo")
		}
	})

	t.Run("query between on primary key", func(t *testing.T) {
		posts, err := goddb.Query(&Post{Author: "abc"}).
			Between(&Post{ID: "a"}, &Post{ID: "b"}).
			Exec()
		assertEq(t, err, nil)
		assertEq(t, len(posts), 1)
		for _, post := range posts {
			assertEq(t, post.Author, "abc")
			assertEq(t, post.ID, "abc")
			assertEq(t, post.Body, "Foo bar")
			assertEq(t, post.Category, "foo")
		}
	})

	t.Run("query GSI", func(t *testing.T) {
		posts, err := goddb.Query(&Post{Category: "foo"}).Exec()
		assertEq(t, err, nil)
		assertEq(t, len(posts), 1)
		for _, post := range posts {
			assertEq(t, post.Author, "abc")
			assertEq(t, len(post.ID), 3)
			assertEq(t, post.Body, "Foo bar")
			assertEq(t, post.Category, "foo")
		}
	})

	t.Run("query scan simple GSI", func(t *testing.T) {
		users, err := goddb.Query(&User{}).Exec()
		assertEq(t, err, nil)
		assertEq(t, len(users), 1)
		for _, user := range users {
			assertEq(t, user.Name, "Jon Doe")
			assertEq(t, user.ID, "abc")
		}
	})

	t.Run("query ambiguous hash", func(t *testing.T) {
		_, err := goddb.Query(&Post{Author: "abc", Category: "foo"}).Exec()
		assertNe(t, err, nil)
		assertContains(t, err.Error(), "ambiguous")
	})

	t.Run("query no hash", func(t *testing.T) {
		_, err := goddb.Query(&Post{}).Exec()
		assertNe(t, err, nil)
	})

	t.Run("delete", func(t *testing.T) {
		assertEq(t, goddb.Delete(&User{ID: "abc"}).Exec(), nil)
		assertEq(t, goddb.Delete(&Post{Author: "abc", ID: "abc"}).Exec(), nil)
		assertEq(t, goddb.Delete(&Post{Author: "abc", ID: "def"}).Exec(), nil)
		_, err := goddb.Get(&User{ID: "abc"}).Exec()
		assertEq(t, err, goddb.ErrItemNotFound)
		_, err = goddb.Get(&Post{Author: "abc", ID: "abc"}).Exec()
		assertEq(t, err, goddb.ErrItemNotFound)
		_, err = goddb.Get(&Post{Author: "abc", ID: "def"}).Exec()
		assertEq(t, err, goddb.ErrItemNotFound)
	})
}

func TestDeleteAll(t *testing.T) {
	type User struct {
		ID   string `goddb:"PK,SK,UserGSI"`
		Name string
	}

	t.Run("delete all users", func(t *testing.T) {
		assertEq(t, goddb.Put(&User{ID: "abc", Name: "Jon Doe"}).Exec(), nil)
		assertEq(t, goddb.Put(&User{ID: "def", Name: "Jane Doe"}).Exec(), nil)
		assertEq(t, goddb.DeleteAll(&User{}).Exec(), nil)
		_, err := goddb.Get(&User{ID: "abc"}).Exec()
		assertEq(t, err, goddb.ErrItemNotFound)
		_, err = goddb.Get(&User{ID: "def"}).Exec()
		assertEq(t, err, goddb.ErrItemNotFound)
	})

	type Post struct {
		ID     string `goddb:"SK,GSI1SK"`
		Author string `goddb:"PK"`
	}
	t.Run("delete all posts", func(t *testing.T) {
		assertEq(t, goddb.Put(&Post{ID: "abc", Author: "abc"}).Exec(), nil)
		assertEq(t, goddb.Put(&Post{ID: "def", Author: "abc"}).Exec(), nil)
		assertEq(t, goddb.Put(&Post{ID: "ghi", Author: "def"}).Exec(), nil)
		assertEq(t, goddb.DeleteAll(&Post{Author: "abc"}).Exec(), nil)
		_, err := goddb.Get(&Post{ID: "abc", Author: "abc"}).Exec()
		assertEq(t, err, goddb.ErrItemNotFound)
		_, err = goddb.Get(&Post{ID: "def", Author: "abc"}).Exec()
		assertEq(t, err, goddb.ErrItemNotFound)
		_, err = goddb.Get(&Post{ID: "ghi", Author: "def"}).Exec()
		assertEq(t, err, nil)
		assertEq(t, goddb.DeleteAll(&Post{Author: "def"}).Exec(), nil)
		_, err = goddb.Get(&Post{ID: "ghi", Author: "def"}).Exec()
		assertEq(t, err, goddb.ErrItemNotFound)
	})

}

func TestCustomTagChar(t *testing.T) {
	goddb.TagChar = ':'
	type User struct {
		ID   string `goddb:"PK,SK,UserGSI"`
		Name string
	}
	assertEq(t, goddb.Put(&User{ID: "abc#def", Name: "Jon Doe"}).Exec(), nil)
	user, err := goddb.Get(&User{ID: "abc#def"}).Exec()
	assertEq(t, err, nil)
	assertEq(t, user.ID, "abc#def")
	assertEq(t, user.Name, "Jon Doe")
	assertEq(t, goddb.Delete(&User{ID: "abc#def"}).Exec(), nil)
	_, err = goddb.Get(&User{ID: "abc#def"}).Consistent().Exec()
	assertEq(t, err, goddb.ErrItemNotFound)
	goddb.TagChar = '#'
}

func TestComplexPKZeroValueQuery(t *testing.T) {
	type User struct {
		ID   int    `goddb:"PK"`
		Org  int    `goddb:"PK"`
		Name string `goddb:"SK,GSI1SK"`
		Foo  string `goddb:"GSI1PK"`
	}
	assertEq(t, goddb.Put(&User{ID: 1, Org: 1, Name: "Jon Doe", Foo: "foo"}).Exec(), nil)
	assertEq(t, goddb.Put(&User{ID: 2, Org: 1, Name: "Jane Doe", Foo: "foo"}).Exec(), nil)

	users, err := goddb.Query(&User{Foo: "foo"}).Exec()
	assertEq(t, err, nil)
	assertEq(t, len(users), 2)

	assertEq(t, goddb.Delete(&User{ID: 1, Org: 1, Name: "Jon Doe"}).Exec(), nil)
	_, err = goddb.Get(&User{ID: 1, Org: 1, Name: "Jon Doe"}).Consistent().Exec()
	assertEq(t, err, goddb.ErrItemNotFound)
	assertEq(t, goddb.Delete(&User{ID: 2, Org: 1, Name: "Jane Doe"}).Exec(), nil)
	_, err = goddb.Get(&User{ID: 2, Org: 1, Name: "Jane Doe"}).Consistent().Exec()
	assertEq(t, err, goddb.ErrItemNotFound)
}

func TestNumbers(t *testing.T) {
	type Int struct {
		ID  int `goddb:"PK,SK"`
		Num int
	}
	assertEq(t, goddb.Put(&Int{ID: -1, Num: -2}).Exec(), nil)
	outputInt, err := goddb.Get(&Int{ID: -1}).Exec()
	assertEq(t, err, nil)
	assertEq(t, outputInt.Num, -2)
	assertEq(t, goddb.Delete(&Int{ID: -1}).Exec(), nil)
	_, err = goddb.Get(&Int{ID: -1}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type Uint struct {
		ID  uint `goddb:"PK,SK"`
		Num uint
	}
	assertEq(t, goddb.Put(&Uint{ID: 1, Num: 2}).Exec(), nil)
	outputUint, err := goddb.Get(&Uint{ID: 1}).Exec()
	assertEq(t, err, nil)
	assertEq(t, outputUint.Num, 2)
	assertEq(t, goddb.Delete(&Uint{ID: 1}).Exec(), nil)
	_, err = goddb.Get(&Uint{ID: 1}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type Int8 struct {
		ID  int8 `goddb:"PK,SK"`
		Num int8
	}
	assertEq(t, goddb.Put(&Int8{ID: -1, Num: -2}).Exec(), nil)
	outputInt8, err := goddb.Get(&Int8{ID: -1}).Exec()
	assertEq(t, err, nil)
	assertEq(t, outputInt8.Num, -2)
	assertEq(t, goddb.Delete(&Int8{ID: -1}).Exec(), nil)
	_, err = goddb.Get(&Int8{ID: -1}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type Uint8 struct {
		ID  uint8 `goddb:"PK,SK"`
		Num uint8
	}
	assertEq(t, goddb.Put(&Uint8{ID: 1, Num: 2}).Exec(), nil)
	outputUint8, err := goddb.Get(&Uint8{ID: 1}).Exec()
	assertEq(t, err, nil)
	assertEq(t, outputUint8.Num, 2)
	assertEq(t, goddb.Delete(&Uint8{ID: 1}).Exec(), nil)
	_, err = goddb.Get(&Uint8{ID: 1}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type Int16 struct {
		ID  int16 `goddb:"PK,SK"`
		Num int16
	}
	assertEq(t, goddb.Put(&Int16{ID: -1, Num: -2}).Exec(), nil)
	outputInt16, err := goddb.Get(&Int16{ID: -1}).Exec()
	assertEq(t, err, nil)
	assertEq(t, outputInt16.Num, -2)
	assertEq(t, goddb.Delete(&Int16{ID: -1}).Exec(), nil)
	_, err = goddb.Get(&Int16{ID: -1}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type Uint16 struct {
		ID  uint16 `goddb:"PK,SK"`
		Num uint16
	}
	assertEq(t, goddb.Put(&Uint16{ID: 1, Num: 2}).Exec(), nil)
	outputUint16, err := goddb.Get(&Uint16{ID: 1}).Exec()
	assertEq(t, err, nil)
	assertEq(t, outputUint16.Num, 2)
	assertEq(t, goddb.Delete(&Uint16{ID: 1}).Exec(), nil)
	_, err = goddb.Get(&Uint16{ID: 1}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type Int32 struct {
		ID  int32 `goddb:"PK,SK"`
		Num int32
	}
	assertEq(t, goddb.Put(&Int32{ID: -1, Num: -2}).Exec(), nil)
	outputInt32, err := goddb.Get(&Int32{ID: -1}).Exec()
	assertEq(t, err, nil)
	assertEq(t, outputInt32.Num, -2)
	assertEq(t, goddb.Delete(&Int32{ID: -1}).Exec(), nil)
	_, err = goddb.Get(&Int32{ID: -1}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type Uint32 struct {
		ID  uint32 `goddb:"PK,SK"`
		Num uint32
	}
	assertEq(t, goddb.Put(&Uint32{ID: 1, Num: 2}).Exec(), nil)
	outputUint32, err := goddb.Get(&Uint32{ID: 1}).Exec()
	assertEq(t, err, nil)
	assertEq(t, outputUint32.Num, 2)
	assertEq(t, goddb.Delete(&Uint32{ID: 1}).Exec(), nil)
	_, err = goddb.Get(&Uint32{ID: 1}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type Int64 struct {
		ID  int64 `goddb:"PK,SK"`
		Num int64
	}
	assertEq(t, goddb.Put(&Int64{ID: -1, Num: -2}).Exec(), nil)
	outputInt64, err := goddb.Get(&Int64{ID: -1}).Exec()
	assertEq(t, err, nil)
	assertEq(t, outputInt64.Num, -2)
	assertEq(t, goddb.Delete(&Int64{ID: -1}).Exec(), nil)
	_, err = goddb.Get(&Int64{ID: -1}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type Uint64 struct {
		ID  uint64 `goddb:"PK,SK"`
		Num uint64
	}
	assertEq(t, goddb.Put(&Uint64{ID: 1, Num: 2}).Exec(), nil)
	outputUint64, err := goddb.Get(&Uint64{ID: 1}).Exec()
	assertEq(t, err, nil)
	assertEq(t, outputUint64.Num, 2)
	assertEq(t, goddb.Delete(&Uint64{ID: 1}).Exec(), nil)
	_, err = goddb.Get(&Uint64{ID: 1}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type Float32 struct {
		ID  float32 `goddb:"PK,SK"`
		Num float32
	}
	assertEq(t, goddb.Put(&Float32{ID: -1.1, Num: -2.2}).Exec(), nil)
	outputFloat32, err := goddb.Get(&Float32{ID: -1.1}).Exec()
	assertEq(t, err, nil)
	assertEq(t, outputFloat32.Num, -2.2)
	assertEq(t, goddb.Delete(&Float32{ID: -1.1}).Exec(), nil)
	_, err = goddb.Get(&Float32{ID: -1.1}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type Float64 struct {
		ID  float64 `goddb:"PK,SK"`
		Num float64
	}
	assertEq(t, goddb.Put(&Float64{ID: -1.1, Num: -2.2}).Exec(), nil)
	outputFloat64, err := goddb.Get(&Float64{ID: -1.1}).Exec()
	assertEq(t, err, nil)
	assertEq(t, outputFloat64.Num, -2.2)
	assertEq(t, goddb.Delete(&Float64{ID: -1.1}).Exec(), nil)
	_, err = goddb.Get(&Float64{ID: -1.1}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)
}

func TestSets(t *testing.T) {
	type StringSet struct {
		ID     string `goddb:"PK,SK"`
		Values []string
	}
	assertEq(t, goddb.Put(&StringSet{ID: "abc", Values: []string{"foo", "bar", "baz"}}).Exec(), nil)
	outputStringSet, err := goddb.Get(&StringSet{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, nil)
	assertEq(t, len(outputStringSet.Values), 3)
	assertEq(t, goddb.Delete(&StringSet{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&StringSet{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type IntSet struct {
		ID     string `goddb:"PK,SK"`
		Values []int
	}
	assertEq(t, goddb.Put(&IntSet{ID: "abc", Values: []int{1, 2, 3}}).Exec(), nil)
	outputIntSet, err := goddb.Get(&IntSet{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, nil)
	assertEq(t, len(outputIntSet.Values), 3)
	assertEq(t, goddb.Delete(&IntSet{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&IntSet{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, goddb.ErrItemNotFound)

	type FloatSet struct {
		ID     string `goddb:"PK,SK"`
		Values []float32
	}
	assertEq(t, goddb.Put(&FloatSet{ID: "abc", Values: []float32{1.1, 2.2, 3.3}}).Exec(), nil)
	outputFloatSet, err := goddb.Get(&FloatSet{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, nil)
	assertEq(t, len(outputFloatSet.Values), 3)
	assertEq(t, goddb.Delete(&FloatSet{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&FloatSet{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, goddb.ErrItemNotFound)
}

func TestBool(t *testing.T) {
	type Bool struct {
		ID    string `goddb:"PK,SK"`
		Value bool
	}
	assertEq(t, goddb.Put(&Bool{ID: "abc", Value: true}).Exec(), nil)
	output, err := goddb.Get(&Bool{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, nil)
	assertEq(t, output.Value, true)
	assertEq(t, goddb.Update(&Bool{ID: "abc"}).Remove(func(t *Bool) any { return t.Value }).Exec(), nil)
	output, err = goddb.Get(&Bool{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, nil)
	assertEq(t, output.Value, false)
	assertEq(t, goddb.Delete(&Bool{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&Bool{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, goddb.ErrItemNotFound)
}

func TestTime(t *testing.T) {
	type Time struct {
		ID    time.Time `goddb:"PK,SK"`
		Value time.Time
	}
	id := time.Now()
	value := time.Now()
	assertEq(t, goddb.Put(&Time{ID: id, Value: value}).Exec(), nil)
	output, err := goddb.Get(&Time{ID: id}).Consistent().Exec()
	assertEq(t, err, nil)
	assertEq(t, output.Value.UTC().Format(time.RFC3339Nano), value.UTC().Format(time.RFC3339Nano))
	assertEq(t, goddb.Update(&Time{ID: id}).Remove(func(t *Time) any { return t.Value }).Exec(), nil)
	output, err = goddb.Get(&Time{ID: id}).Consistent().Exec()
	assertEq(t, err, nil)
	assertEq(t, output.Value.UTC().Format(time.RFC3339Nano), time.Time{}.UTC().Format(time.RFC3339Nano))
	assertEq(t, goddb.Delete(&Time{ID: id}).Exec(), nil)
	_, err = goddb.Get(&Time{ID: id}).Consistent().Exec()
	assertEq(t, err, goddb.ErrItemNotFound)
}

func TestUpdateSet(t *testing.T) {
	type Update struct {
		ID  string `goddb:"PK,SK"`
		Foo string
	}
	assertEq(t, goddb.Put(&Update{ID: "abc", Foo: "foo"}).Exec(), nil)
	assertEq(t, goddb.Update(&Update{ID: "abc"}).Set(&Update{Foo: "bar"}).Exec(), nil)
	update, err := goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, nil)
	assertEq(t, update.Foo, "bar")
	assertEq(t, goddb.Delete(&Update{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&Update{ID: "abc"}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)
}

func TestUpdateAdd(t *testing.T) {
	type Update struct {
		ID  string `goddb:"PK,SK"`
		Foo int
	}
	assertEq(t, goddb.Put(&Update{ID: "abc", Foo: 1}).Exec(), nil)
	assertEq(t, goddb.Update(&Update{ID: "abc"}).Add(&Update{Foo: 2}).Exec(), nil)
	update, err := goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, nil)
	assertEq(t, update.Foo, 3)
	assertEq(t, goddb.Delete(&Update{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&Update{ID: "abc"}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)
}

func TestUpdateRemove(t *testing.T) {
	type Update struct {
		ID  string `goddb:"PK,SK"`
		Foo int
		Bar string
		Baz float64
	}
	assertEq(t, goddb.Put(&Update{ID: "abc", Foo: 1, Bar: "bar", Baz: 1.234}).Exec(), nil)
	assertEq(t, goddb.Update(&Update{ID: "abc"}).
		Remove(func(u *Update) any { return u.Foo }).
		Remove(func(u *Update) any { return u.Bar }).
		Remove(func(u *Update) any { return u.Baz }).
		Exec(), nil)
	update, err := goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, nil)
	assertEq(t, update.Foo, 0)
	assertEq(t, goddb.Delete(&Update{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&Update{ID: "abc"}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)
}

func TestUpdateDelete(t *testing.T) {
	type Update struct {
		ID  string `goddb:"PK,SK"`
		Set []string
	}
	assertEq(t, goddb.Put(&Update{ID: "abc", Set: []string{"foo", "bar", "baz"}}).Exec(), nil)
	assertEq(t, goddb.Update(&Update{ID: "abc"}).
		Delete(&Update{Set: []string{"bar", "baz"}}).
		Exec(), nil)
	update, err := goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
	assertEq(t, err, nil)
	assertEq(t, len(update.Set), 1)
	assertEq(t, update.Set[0], "foo")
	assertEq(t, goddb.Delete(&Update{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&Update{ID: "abc"}).Exec()
	assertEq(t, err, goddb.ErrItemNotFound)
}
