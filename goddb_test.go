package goddb_test

import (
	"goddb"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
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
		assert.Equal(t, goddb.Put(&User{ID: "abc", Name: "Jon Doe"}).Exec(), nil)
	})

	t.Run("get user", func(t *testing.T) {
		user, err := goddb.Get(&User{ID: "abc"}).Consistent().Exec()
		assert.Equal(t, err, nil)
		assert.Equal(t, user.ID, "abc")
		assert.Equal(t, user.Name, "Jon Doe")
	})

	t.Run("put two valid posts", func(t *testing.T) {
		assert.Equal(t, goddb.Put(&Post{Author: "abc", ID: "abc", Body: "Foo bar", Category: "foo"}).Exec(), nil)
		assert.Equal(t, goddb.Put(&Post{Author: "abc", ID: "def", Body: "Baz bat", Category: "baz"}).Exec(), nil)
	})

	t.Run("put invalid valid posts", func(t *testing.T) {
		type Invalid struct {
			ID  string `goddb:"PK,SK"`
			Foo string `goddb:"SK"`
		}
		assert.NotEqual(t, goddb.Put(&User{}).Exec(), nil)
		assert.NotEqual(t, goddb.Put(&Post{}).Exec(), nil)
		assert.NotEqual(t, goddb.Put(&Post{Author: "abc"}).Exec(), nil)
		assert.NotEqual(t, goddb.Put(&Post{ID: "abc"}).Exec(), nil)
		assert.NotEqual(t, goddb.Put(&Invalid{}).Exec(), nil)
		assert.NotEqual(t, goddb.Put(&Invalid{ID: "abc"}).Exec(), nil)
		assert.NotEqual(t, goddb.Put(&Invalid{Foo: "abc"}).Exec(), nil)
		assert.NotEqual(t, goddb.Put(&Invalid{ID: "abc", Foo: "foo"}).Exec(), nil)
	})

	t.Run("query all on primary key", func(t *testing.T) {
		posts, err := goddb.Query(&Post{Author: "abc"}).Exec()
		assert.Equal(t, err, nil)
		assert.Equal(t, len(posts), 2)
		for _, post := range posts {
			assert.Equal(t, post.Author, "abc")
			assert.Equal(t, len(post.ID), 3)
			assert.Equal(t, len(post.Body), 7)
			assert.Equal(t, len(post.Category), 3)
		}
	})

	t.Run("query with pagination", func(t *testing.T) {
		var offset string
		posts, err := goddb.Query(&Post{Author: "abc"}).Page(1, &offset).Exec()
		assert.Equal(t, err, nil)
		assert.Equal(t, len(posts), 1)
		assert.Equal(t, posts[0].ID, "abc")
		posts, err = goddb.Query(&Post{Author: "abc"}).Page(1, &offset).Exec()
		assert.Equal(t, err, nil)
		assert.Equal(t, len(posts), 1)
		assert.Equal(t, posts[0].ID, "def")
		posts, err = goddb.Query(&Post{Author: "abc"}).Page(1, &offset).Exec()
		assert.Equal(t, err, nil)
		assert.Equal(t, len(posts), 0)
		assert.Equal(t, offset, "")
	})

	t.Run("query begins with on primary key", func(t *testing.T) {
		posts, err := goddb.Query(&Post{Author: "abc"}).
			BeginsWith(&Post{ID: "a"}).
			Exec()
		assert.Equal(t, err, nil)
		assert.Equal(t, len(posts), 1)
		for _, post := range posts {
			assert.Equal(t, post.Author, "abc")
			assert.Equal(t, post.ID, "abc")
			assert.Equal(t, post.Body, "Foo bar")
			assert.Equal(t, post.Category, "foo")
		}
	})

	t.Run("query between on primary key", func(t *testing.T) {
		posts, err := goddb.Query(&Post{Author: "abc"}).
			Between(&Post{ID: "a"}, &Post{ID: "b"}).
			Exec()
		assert.Equal(t, err, nil)
		assert.Equal(t, len(posts), 1)
		for _, post := range posts {
			assert.Equal(t, post.Author, "abc")
			assert.Equal(t, post.ID, "abc")
			assert.Equal(t, post.Body, "Foo bar")
			assert.Equal(t, post.Category, "foo")
		}
	})

	t.Run("query GSI", func(t *testing.T) {
		posts, err := goddb.Query(&Post{Category: "foo"}).Exec()
		assert.Equal(t, err, nil)
		assert.Equal(t, len(posts), 1)
		for _, post := range posts {
			assert.Equal(t, post.Author, "abc")
			assert.Equal(t, len(post.ID), 3)
			assert.Equal(t, post.Body, "Foo bar")
			assert.Equal(t, post.Category, "foo")
		}
	})

	t.Run("query scan simple GSI", func(t *testing.T) {
		users, err := goddb.Query(&User{}).Exec()
		assert.Equal(t, err, nil)
		assert.Equal(t, len(users), 1)
		for _, user := range users {
			assert.Equal(t, user.Name, "Jon Doe")
			assert.Equal(t, user.ID, "abc")
		}
	})

	t.Run("query scan simple GSI not exists", func(t *testing.T) {
		_, err := goddb.Query(&Post{}).Exec()
		assert.ErrorContains(t, err, "index")
	})

	t.Run("query ambiguous hash", func(t *testing.T) {
		_, err := goddb.Query(&Post{Author: "abc", Category: "foo"}).Exec()
		assert.NotEqual(t, err, nil)
		assert.ErrorContains(t, err, "ambiguous")
	})

	t.Run("query no hash", func(t *testing.T) {
		_, err := goddb.Query(&Post{}).Exec()
		assert.NotEqual(t, err, nil)
	})

	t.Run("delete", func(t *testing.T) {
		assert.Equal(t, goddb.Delete(&User{ID: "abc"}).Exec(), nil)
		assert.Equal(t, goddb.Delete(&Post{Author: "abc", ID: "abc"}).Exec(), nil)
		assert.Equal(t, goddb.Delete(&Post{Author: "abc", ID: "def"}).Exec(), nil)
		_, err := goddb.Get(&User{ID: "abc"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
		_, err = goddb.Get(&Post{Author: "abc", ID: "abc"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
		_, err = goddb.Get(&Post{Author: "abc", ID: "def"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
	})
}

func TestDeleteAll(t *testing.T) {
	type User struct {
		ID   string `goddb:"PK,SK,UserGSI"`
		Name string
	}

	t.Run("delete all users", func(t *testing.T) {
		assert.Equal(t, goddb.Put(&User{ID: "abc", Name: "Jon Doe"}).Exec(), nil)
		assert.Equal(t, goddb.Put(&User{ID: "def", Name: "Jane Doe"}).Exec(), nil)
		assert.Equal(t, goddb.DeleteAll(&User{}).Exec(), nil)
		_, err := goddb.Get(&User{ID: "abc"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
		_, err = goddb.Get(&User{ID: "def"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
	})

	type Post struct {
		ID     string `goddb:"SK,GSI1SK"`
		Author string `goddb:"PK"`
	}
	t.Run("delete all posts", func(t *testing.T) {
		assert.Equal(t, goddb.Put(&Post{ID: "abc", Author: "abc"}).Exec(), nil)
		assert.Equal(t, goddb.Put(&Post{ID: "def", Author: "abc"}).Exec(), nil)
		assert.Equal(t, goddb.Put(&Post{ID: "ghi", Author: "def"}).Exec(), nil)
		assert.Equal(t, goddb.DeleteAll(&Post{Author: "abc"}).Exec(), nil)
		_, err := goddb.Get(&Post{ID: "abc", Author: "abc"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
		_, err = goddb.Get(&Post{ID: "def", Author: "abc"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
		_, err = goddb.Get(&Post{ID: "ghi", Author: "def"}).Consistent().Exec()
		assert.Equal(t, err, nil)
		assert.Equal(t, goddb.DeleteAll(&Post{Author: "def"}).Exec(), nil)
		_, err = goddb.Get(&Post{ID: "ghi", Author: "def"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
	})

}

func TestCustomTagChar(t *testing.T) {
	goddb.TagChar = ':'
	type User struct {
		ID   string `goddb:"PK,SK,UserGSI"`
		Name string
	}
	assert.Equal(t, goddb.Put(&User{ID: "abc#def", Name: "Jon Doe"}).Exec(), nil)
	user, err := goddb.Get(&User{ID: "abc#def"}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, user.ID, "abc#def")
	assert.Equal(t, user.Name, "Jon Doe")
	assert.Equal(t, goddb.Delete(&User{ID: "abc#def"}).Exec(), nil)
	_, err = goddb.Get(&User{ID: "abc#def"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)
	goddb.TagChar = '#'
	assert.ErrorContains(t, goddb.Put(&User{ID: "abc#def", Name: "Jon Doe"}).Exec(), "tag char")
}

func TestComplexPKZeroValueQuery(t *testing.T) {
	type User struct {
		ID   int    `goddb:"PK"`
		Org  int    `goddb:"PK"`
		Name string `goddb:"SK,GSI1SK"`
		Foo  string `goddb:"GSI1PK"`
	}
	assert.Equal(t, goddb.Put(&User{ID: 1, Org: 1, Name: "Jon Doe", Foo: "foo"}).Exec(), nil)
	assert.Equal(t, goddb.Put(&User{ID: 2, Org: 1, Name: "Jane Doe", Foo: "foo"}).Exec(), nil)

	users, err := goddb.Query(&User{Foo: "foo"}).Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(users), 2)

	assert.Equal(t, goddb.Delete(&User{ID: 1, Org: 1, Name: "Jon Doe"}).Exec(), nil)
	_, err = goddb.Get(&User{ID: 1, Org: 1, Name: "Jon Doe"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)
	assert.Equal(t, goddb.Delete(&User{ID: 2, Org: 1, Name: "Jane Doe"}).Exec(), nil)
	_, err = goddb.Get(&User{ID: 2, Org: 1, Name: "Jane Doe"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)
}

func TestNumbers(t *testing.T) {
	type Int struct {
		ID  int `goddb:"PK,SK"`
		Num int
	}
	assert.Equal(t, goddb.Put(&Int{ID: -1, Num: -2}).Exec(), nil)
	outputInt, err := goddb.Get(&Int{ID: -1}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputInt.Num, int(-2))
	assert.Equal(t, goddb.Delete(&Int{ID: -1}).Exec(), nil)
	_, err = goddb.Get(&Int{ID: -1}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type Uint struct {
		ID  uint `goddb:"PK,SK"`
		Num uint
	}
	assert.Equal(t, goddb.Put(&Uint{ID: 1, Num: 2}).Exec(), nil)
	outputUint, err := goddb.Get(&Uint{ID: 1}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputUint.Num, uint(2))
	assert.Equal(t, goddb.Delete(&Uint{ID: 1}).Exec(), nil)
	_, err = goddb.Get(&Uint{ID: 1}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type Int8 struct {
		ID  int8 `goddb:"PK,SK"`
		Num int8
	}
	assert.Equal(t, goddb.Put(&Int8{ID: -1, Num: -2}).Exec(), nil)
	outputInt8, err := goddb.Get(&Int8{ID: -1}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputInt8.Num, int8(-2))
	assert.Equal(t, goddb.Delete(&Int8{ID: -1}).Exec(), nil)
	_, err = goddb.Get(&Int8{ID: -1}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type Uint8 struct {
		ID  uint8 `goddb:"PK,SK"`
		Num uint8
	}
	assert.Equal(t, goddb.Put(&Uint8{ID: 1, Num: 2}).Exec(), nil)
	outputUint8, err := goddb.Get(&Uint8{ID: 1}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputUint8.Num, uint8(2))
	assert.Equal(t, goddb.Delete(&Uint8{ID: 1}).Exec(), nil)
	_, err = goddb.Get(&Uint8{ID: 1}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type Int16 struct {
		ID  int16 `goddb:"PK,SK"`
		Num int16
	}
	assert.Equal(t, goddb.Put(&Int16{ID: -1, Num: -2}).Exec(), nil)
	outputInt16, err := goddb.Get(&Int16{ID: -1}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputInt16.Num, int16(-2))
	assert.Equal(t, goddb.Delete(&Int16{ID: -1}).Exec(), nil)
	_, err = goddb.Get(&Int16{ID: -1}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type Uint16 struct {
		ID  uint16 `goddb:"PK,SK"`
		Num uint16
	}
	assert.Equal(t, goddb.Put(&Uint16{ID: 1, Num: 2}).Exec(), nil)
	outputUint16, err := goddb.Get(&Uint16{ID: 1}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputUint16.Num, uint16(2))
	assert.Equal(t, goddb.Delete(&Uint16{ID: 1}).Exec(), nil)
	_, err = goddb.Get(&Uint16{ID: 1}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type Int32 struct {
		ID  int32 `goddb:"PK,SK"`
		Num int32
	}
	assert.Equal(t, goddb.Put(&Int32{ID: -1, Num: -2}).Exec(), nil)
	outputInt32, err := goddb.Get(&Int32{ID: -1}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputInt32.Num, int32(-2))
	assert.Equal(t, goddb.Delete(&Int32{ID: -1}).Exec(), nil)
	_, err = goddb.Get(&Int32{ID: -1}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type Uint32 struct {
		ID  uint32 `goddb:"PK,SK"`
		Num uint32
	}
	assert.Equal(t, goddb.Put(&Uint32{ID: 1, Num: 2}).Exec(), nil)
	outputUint32, err := goddb.Get(&Uint32{ID: 1}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputUint32.Num, uint32(2))
	assert.Equal(t, goddb.Delete(&Uint32{ID: 1}).Exec(), nil)
	_, err = goddb.Get(&Uint32{ID: 1}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type Int64 struct {
		ID  int64 `goddb:"PK,SK"`
		Num int64
	}
	assert.Equal(t, goddb.Put(&Int64{ID: -1, Num: -2}).Exec(), nil)
	outputInt64, err := goddb.Get(&Int64{ID: -1}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputInt64.Num, int64(-2))
	assert.Equal(t, goddb.Delete(&Int64{ID: -1}).Exec(), nil)
	_, err = goddb.Get(&Int64{ID: -1}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type Uint64 struct {
		ID  uint64 `goddb:"PK,SK"`
		Num uint64
	}
	assert.Equal(t, goddb.Put(&Uint64{ID: 1, Num: 2}).Exec(), nil)
	outputUint64, err := goddb.Get(&Uint64{ID: 1}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputUint64.Num, uint64(2))
	assert.Equal(t, goddb.Delete(&Uint64{ID: 1}).Exec(), nil)
	_, err = goddb.Get(&Uint64{ID: 1}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type Float32 struct {
		ID  float32 `goddb:"PK,SK"`
		Num float32
	}
	assert.Equal(t, goddb.Put(&Float32{ID: -1.1, Num: -2.2}).Exec(), nil)
	outputFloat32, err := goddb.Get(&Float32{ID: -1.1}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputFloat32.Num, float32(-2.2))
	assert.Equal(t, goddb.Delete(&Float32{ID: -1.1}).Exec(), nil)
	_, err = goddb.Get(&Float32{ID: -1.1}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type Float64 struct {
		ID  float64 `goddb:"PK,SK"`
		Num float64
	}
	assert.Equal(t, goddb.Put(&Float64{ID: -1.1, Num: -2.2}).Exec(), nil)
	outputFloat64, err := goddb.Get(&Float64{ID: -1.1}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputFloat64.Num, float64(-2.2))
	assert.Equal(t, goddb.Delete(&Float64{ID: -1.1}).Exec(), nil)
	_, err = goddb.Get(&Float64{ID: -1.1}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)
}

// test time.Duration
func TestDuration(t *testing.T) {
	type Duration struct {
		ID  string `goddb:"PK,SK"`
		Num time.Duration
	}
	assert.Equal(t, goddb.Put(&Duration{ID: "abc", Num: 10 * time.Second}).Exec(), nil)
	outputDuration, err := goddb.Get(&Duration{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, outputDuration.Num, 10*time.Second)
	assert.Equal(t, goddb.Delete(&Duration{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&Duration{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)
}

func TestSets(t *testing.T) {
	type StringSet struct {
		ID     string `goddb:"PK,SK"`
		Values []string
	}
	assert.Equal(t, goddb.Put(&StringSet{ID: "abc", Values: []string{"foo", "bar", "baz"}}).Exec(), nil)
	outputStringSet, err := goddb.Get(&StringSet{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(outputStringSet.Values), 3)
	assert.Equal(t, goddb.Delete(&StringSet{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&StringSet{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type IntSet struct {
		ID     string `goddb:"PK,SK"`
		Values []int
	}
	assert.Equal(t, goddb.Put(&IntSet{ID: "abc", Values: []int{1, 2, 3}}).Exec(), nil)
	outputIntSet, err := goddb.Get(&IntSet{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(outputIntSet.Values), 3)
	assert.Equal(t, goddb.Delete(&IntSet{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&IntSet{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type FloatSet struct {
		ID     string `goddb:"PK,SK"`
		Values []float32
	}
	assert.Equal(t, goddb.Put(&FloatSet{ID: "abc", Values: []float32{1.1, 2.2, 3.3}}).Exec(), nil)
	outputFloatSet, err := goddb.Get(&FloatSet{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(outputFloatSet.Values), 3)
	assert.Equal(t, goddb.Delete(&FloatSet{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&FloatSet{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)

	type TimeSet struct {
		ID     string `goddb:"PK,SK"`
		Values []time.Time
	}
	a := time.Now()
	b := a.Add(time.Second)
	c := b.Add(time.Second)
	assert.Equal(t, goddb.Put(&TimeSet{ID: "abc", Values: []time.Time{a, b, c}}).Exec(), nil)
	outputTimeSet, err := goddb.Get(&TimeSet{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(outputTimeSet.Values), 3)
	assert.Equal(t, goddb.Delete(&TimeSet{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&TimeSet{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)
}

func TestBool(t *testing.T) {
	type Bool struct {
		ID    string `goddb:"PK,SK"`
		Value bool
	}
	assert.Equal(t, goddb.Put(&Bool{ID: "abc", Value: true}).Exec(), nil)
	output, err := goddb.Get(&Bool{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, output.Value, true)
	assert.Equal(t, goddb.Update(&Bool{ID: "abc"}).Remove(func(t *Bool) any { return t.Value }).Exec(), nil)
	output, err = goddb.Get(&Bool{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, output.Value, false)
	assert.Equal(t, goddb.Delete(&Bool{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&Bool{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)
}

func TestTime(t *testing.T) {
	type Time struct {
		ID    time.Time `goddb:"PK,SK"`
		Value time.Time
	}
	id := time.Now()
	value := time.Now()
	assert.Equal(t, goddb.Put(&Time{ID: id, Value: value}).Exec(), nil)
	output, err := goddb.Get(&Time{ID: id}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, output.Value.UTC().Format(time.RFC3339Nano), value.UTC().Format(time.RFC3339Nano))
	assert.Equal(t, goddb.Update(&Time{ID: id}).Remove(func(t *Time) any { return t.Value }).Exec(), nil)
	output, err = goddb.Get(&Time{ID: id}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, output.Value.UTC().Format(time.RFC3339Nano), time.Time{}.UTC().Format(time.RFC3339Nano))
	assert.Equal(t, goddb.Delete(&Time{ID: id}).Exec(), nil)
	_, err = goddb.Get(&Time{ID: id}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)
}

func TestUpdateSet(t *testing.T) {
	type Update struct {
		ID  string `goddb:"PK,SK"`
		Foo string
	}
	assert.Equal(t, goddb.Put(&Update{ID: "abc", Foo: "foo"}).Exec(), nil)
	assert.Equal(t, goddb.Update(&Update{ID: "abc"}).Set(&Update{Foo: "bar"}).Exec(), nil)
	update, err := goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, update.Foo, "bar")
	assert.Equal(t, goddb.Delete(&Update{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)
}

func TestUpdateAdd(t *testing.T) {
	type Update struct {
		ID  string `goddb:"PK,SK"`
		Foo int
	}
	assert.Equal(t, goddb.Put(&Update{ID: "abc", Foo: 1}).Exec(), nil)
	assert.Equal(t, goddb.Update(&Update{ID: "abc"}).Add(&Update{Foo: 2}).Exec(), nil)
	update, err := goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, update.Foo, 3)
	assert.Equal(t, goddb.Delete(&Update{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)
}

func TestUpdateRemove(t *testing.T) {
	type Update struct {
		ID  string `goddb:"PK,SK"`
		Foo int
		Bar string
		Baz float64
	}
	assert.Equal(t, goddb.Put(&Update{ID: "abc", Foo: 1, Bar: "bar", Baz: 1.234}).Exec(), nil)
	assert.Equal(t, goddb.Update(&Update{ID: "abc"}).
		Remove(func(u *Update) any { return u.Foo }).
		Remove(func(u *Update) any { return u.Bar }).
		Remove(func(u *Update) any { return u.Baz }).
		Exec(), nil)
	update, err := goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, update.Foo, 0)
	assert.Equal(t, goddb.Delete(&Update{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)
}

func TestUpdateDelete(t *testing.T) {
	type Update struct {
		ID  string `goddb:"PK,SK"`
		Set []string
	}
	assert.Equal(t, goddb.Put(&Update{ID: "abc", Set: []string{"foo", "bar", "baz"}}).Exec(), nil)
	assert.Equal(t, goddb.Update(&Update{ID: "abc"}).
		Delete(&Update{Set: []string{"bar", "baz"}}).
		Exec(), nil)
	update, err := goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, nil)
	assert.Equal(t, len(update.Set), 1)
	assert.Equal(t, update.Set[0], "foo")
	assert.Equal(t, goddb.Delete(&Update{ID: "abc"}).Exec(), nil)
	_, err = goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
	assert.Equal(t, err, goddb.ErrItemNotFound)
}

func TestConditionals(t *testing.T) {
	t.Run("equals", func(t *testing.T) {
		type Put struct {
			ID  string `goddb:"PK,SK"`
			Foo string
			Bar string
		}
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "foo", Bar: "bar"}).Exec(), nil)
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "bar", Bar: "bar"}).If(goddb.Equal(&Put{Foo: "foo", Bar: "bar"})).Exec(), nil)
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "baz", Bar: "bar"}).If(goddb.Equal(&Put{Foo: "foo", Bar: "bar"})).Exec(), goddb.ErrConditionFailed)
		assert.Equal(t, goddb.Delete(&Put{ID: "abc"}).Exec(), nil)
		_, err := goddb.Get(&Put{ID: "abc"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
	})
	t.Run("not equal", func(t *testing.T) {
		type Put struct {
			ID  string `goddb:"PK,SK"`
			Foo string
		}
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "foo"}).Exec(), nil)
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "bar"}).If(goddb.NotEqual(&Put{Foo: "bar"})).Exec(), nil)
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "baz"}).If(goddb.NotEqual(&Put{Foo: "bar"})).Exec(), goddb.ErrConditionFailed)
		assert.Equal(t, goddb.Delete(&Put{ID: "abc"}).Exec(), nil)
		_, err := goddb.Get(&Put{ID: "abc"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
	})
	t.Run("and", func(t *testing.T) {
		type Put struct {
			ID  string `goddb:"PK,SK"`
			Foo string
			Bar string
		}
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "foo", Bar: "bar"}).Exec(), nil)
		cond := goddb.And(goddb.Equal(&Put{Foo: "foo"}), goddb.NotEqual(&Put{Bar: "baz"}))
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "bar", Bar: "bar"}).If(cond).Exec(), nil)
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "baz", Bar: "bar"}).If(cond).Exec(), goddb.ErrConditionFailed)
		assert.Equal(t, goddb.Delete(&Put{ID: "abc"}).Exec(), nil)
		_, err := goddb.Get(&Put{ID: "abc"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
	})
	t.Run("or", func(t *testing.T) {
		type Put struct {
			ID  string `goddb:"PK,SK"`
			Foo string
			Bar string
		}
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "foo", Bar: "bar"}).Exec(), nil)
		cond := goddb.Or(goddb.Equal(&Put{Foo: "foo"}), goddb.NotEqual(&Put{Bar: "bar"}))
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "bar", Bar: "bar"}).If(cond).Exec(), nil)
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "baz", Bar: "bar"}).If(cond).Exec(), goddb.ErrConditionFailed)
		assert.Equal(t, goddb.Delete(&Put{ID: "abc"}).Exec(), nil)
		_, err := goddb.Get(&Put{ID: "abc"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
	})
	t.Run("complex", func(t *testing.T) {
		type Update struct {
			ID  string `goddb:"PK,SK"`
			Foo string
			Bar string
		}
		assert.Equal(t, goddb.Put(&Update{ID: "abc", Foo: "foo", Bar: "bar"}).Exec(), nil)
		cond := goddb.And(
			goddb.Or(
				goddb.Equal(&Update{Foo: "foo"}),
				goddb.NotEqual(&Update{Bar: "bar"}),
			),
			goddb.And(
				goddb.Equal(&Update{Foo: "foo"}),
				goddb.Equal(&Update{Bar: "bar"}),
			),
		)
		assert.Equal(t, goddb.Put(&Update{ID: "abc", Foo: "bar", Bar: "bar"}).If(cond).Exec(), nil)
		assert.Equal(t, goddb.Put(&Update{ID: "abc", Foo: "baz", Bar: "bar"}).If(cond).Exec(), goddb.ErrConditionFailed)
		assert.Equal(t, goddb.Delete(&Update{ID: "abc"}).Exec(), nil)
		_, err := goddb.Get(&Update{ID: "abc"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
	})
	t.Run("attribute exists", func(t *testing.T) {
		type Put struct {
			ID  string `goddb:"PK,SK"`
			Foo string
			Bar string
		}
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "foo", Bar: "bar"}).Exec(), nil)
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "bar"}).If(goddb.AttributeExists(func(p *Put) any { return p.Bar })).Exec(), nil)
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "bar"}).If(goddb.AttributeExists(func(p *Put) any { return p.Bar })).Exec(), goddb.ErrConditionFailed)
		assert.Equal(t, goddb.Delete(&Put{ID: "abc"}).Exec(), nil)
		_, err := goddb.Get(&Put{ID: "abc"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
	})
	t.Run("attribute not exists", func(t *testing.T) {
		type Put struct {
			ID  string `goddb:"PK,SK"`
			Foo string
			Bar string
		}
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "foo"}).Exec(), nil)
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "foo", Bar: "bar"}).If(goddb.AttributeNotExists(func(p *Put) any { return p.Bar })).Exec(), nil)
		assert.Equal(t, goddb.Put(&Put{ID: "abc", Foo: "bar"}).If(goddb.AttributeNotExists(func(p *Put) any { return p.Bar })).Exec(), goddb.ErrConditionFailed)
		assert.Equal(t, goddb.Delete(&Put{ID: "abc"}).Exec(), nil)
		_, err := goddb.Get(&Put{ID: "abc"}).Consistent().Exec()
		assert.Equal(t, err, goddb.ErrItemNotFound)
	})
}
