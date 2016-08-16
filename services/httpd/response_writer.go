package httpd

import (
	"bufio"
	"bytes"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/ugorji/go/codec"
)

// ResponseWriter is an interface for writing a response.
type ResponseWriter interface {
	// WriteResponse writes a response.
	WriteResponse(resp Response) (int, error)

	http.ResponseWriter
}

// NewResponseWriter creates a new ResponseWriter based on the Accept header
// in the request that wraps the ResponseWriter.
func NewResponseWriter(w http.ResponseWriter, r *http.Request) ResponseWriter {
	pretty := r.URL.Query().Get("pretty") == "true"
	switch r.Header.Get("Accept") {
	case "application/csv", "text/csv":
		w.Header().Add("Content-Type", "text/csv")
		return &csvResponseWriter{statementID: -1, ResponseWriter: w}
	case "application/x-msgpack":
		w.Header().Add("Content-Type", "application/x-msgpack")
		return newMsgpackResponseWriter(w)
	case "application/json":
		fallthrough
	default:
		w.Header().Add("Content-Type", "application/json")
		return &jsonResponseWriter{Pretty: pretty, ResponseWriter: w}
	}
}

// WriteError is a convenience function for writing an error response to the ResponseWriter.
func WriteError(w ResponseWriter, err error) (int, error) {
	return w.WriteResponse(Response{Err: err})
}

type jsonResponseWriter struct {
	Pretty bool
	http.ResponseWriter
}

func (w *jsonResponseWriter) WriteResponse(resp Response) (n int, err error) {
	var b []byte
	if w.Pretty {
		b, err = json.MarshalIndent(resp, "", "    ")
	} else {
		b, err = json.Marshal(resp)
	}

	if err != nil {
		n, err = io.WriteString(w, err.Error())
	} else {
		n, err = w.Write(b)
	}

	w.Write([]byte("\n"))
	n++
	return n, err
}

// Flush flushes the ResponseWriter if it has a Flush() method.
func (w *jsonResponseWriter) Flush() {
	if w, ok := w.ResponseWriter.(http.Flusher); ok {
		w.Flush()
	}
}

type csvResponseWriter struct {
	statementID int
	columns     []string
	http.ResponseWriter
}

func (w *csvResponseWriter) WriteResponse(resp Response) (n int, err error) {
	csv := csv.NewWriter(writer{Writer: w, n: &n})
	defer csv.Flush()
	for _, result := range resp.Results {
		if result.StatementID != w.statementID {
			// If there are no series in the result, skip past this result.
			if len(result.Series) == 0 {
				continue
			}

			// Set the statement id and print out a newline if this is not the first statement.
			if w.statementID >= 0 {
				// Flush the csv writer and write a newline.
				csv.Flush()
				if err := csv.Error(); err != nil {
					return n, err
				}

				if out, err := io.WriteString(w, "\n"); err != nil {
					return n, err
				} else {
					n += out
				}
			}
			w.statementID = result.StatementID

			// Print out the column headers from the first series.
			w.columns = make([]string, 2+len(result.Series[0].Columns))
			w.columns[0] = "name"
			w.columns[1] = "tags"
			copy(w.columns[2:], result.Series[0].Columns)
			if err := csv.Write(w.columns); err != nil {
				return n, err
			}
		}

		for _, row := range result.Series {
			w.columns[0] = row.Name
			if len(row.Tags) > 0 {
				w.columns[1] = string(models.Tags(row.Tags).HashKey()[1:])
			} else {
				w.columns[1] = ""
			}
			for _, values := range row.Values {
				for i, value := range values {
					switch v := value.(type) {
					case float64:
						w.columns[i+2] = strconv.FormatFloat(v, 'f', -1, 64)
					case int64:
						w.columns[i+2] = strconv.FormatInt(v, 10)
					case string:
						w.columns[i+2] = v
					case bool:
						if v {
							w.columns[i+2] = "true"
						} else {
							w.columns[i+2] = "false"
						}
					case time.Time:
						w.columns[i+2] = strconv.FormatInt(v.UnixNano(), 10)
					}
				}
				csv.Write(w.columns)
			}
		}
	}
	csv.Flush()
	if err := csv.Error(); err != nil {
		return n, err
	}
	return n, nil
}

func (w *csvResponseWriter) Flush() {
	if w, ok := w.ResponseWriter.(http.Flusher); ok {
		w.Flush()
	}
}

type msgpackTimeExt struct {
	enc *codec.Encoder
	buf bytes.Buffer
}

func newMsgpackTimeExt(h *codec.MsgpackHandle) *msgpackTimeExt {
	ext := &msgpackTimeExt{}
	ext.enc = codec.NewEncoder(&ext.buf, h)
	return ext
}

func (x *msgpackTimeExt) WriteExt(v interface{}) (data []byte) {
	var t time.Time
	switch v := v.(type) {
	case time.Time:
		t = v
	case *time.Time:
		t = *v
	default:
		panic(fmt.Sprintf("unsupported format for time conversion: expecting time.Time, got %T", v))
	}

	// The codec library does not expose encoding to a byte string directly so
	// we use our own encoder to encode an int. We reuse the internal buffer to
	// reduce the number of allocations. This makes this extension
	// non-threadsafe, but we make a new extension writer every time we create
	// an encoder so this shouldn't matter.
	x.buf.Reset()
	x.enc.MustEncode(t.UnixNano())
	return x.buf.Bytes()
}

func (x *msgpackTimeExt) ReadExt(dst interface{}, src []byte) { panic("unsupported") }

type msgpackResponseWriter struct {
	http.ResponseWriter
	enc *codec.Encoder
	w   *bufio.Writer
	n   int
}

func newMsgpackResponseWriter(rw http.ResponseWriter) *msgpackResponseWriter {
	var mh codec.MsgpackHandle
	mh.WriteExt = true
	mh.SetBytesExt(reflect.TypeOf(time.Time{}), 1, newMsgpackTimeExt(&mh))

	w := &msgpackResponseWriter{ResponseWriter: rw}
	w.w = bufio.NewWriter(w.ResponseWriter)
	w.enc = codec.NewEncoder(writer{Writer: w.w, n: &w.n}, &mh)
	return w
}

func (w *msgpackResponseWriter) WriteResponse(resp Response) (n int, err error) {
	w.n = 0
	err = w.enc.Encode(resp)
	return w.n, err
}

func (w *msgpackResponseWriter) Flush() {
	w.w.Flush()
	if w, ok := w.ResponseWriter.(http.Flusher); ok {
		w.Flush()
	}
}

type writer struct {
	io.Writer
	n *int
}

func (w writer) Write(data []byte) (n int, err error) {
	n, err = w.Writer.Write(data)
	*w.n += n
	return n, err
}
