package httpd_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/influxdata/influxdb/influxql"
	"github.com/influxdata/influxdb/models"
	"github.com/influxdata/influxdb/services/httpd"
)

// discard is an http.ResponseWriter that discards all output.
type discard struct{}

func (discard) Header() http.Header            { return http.Header{} }
func (discard) WriteHeader(int)                {}
func (discard) Write(data []byte) (int, error) { return len(data), nil }

func BenchmarkJSONResponseWriter_1K(b *testing.B) {
	benchmarkResponseWriter(b, "application/json", 10, 100)
}
func BenchmarkJSONResponseWriter_100K(b *testing.B) {
	benchmarkResponseWriter(b, "application/json", 1000, 100)
}
func BenchmarkJSONResponseWriter_1M(b *testing.B) {
	benchmarkResponseWriter(b, "application/json", 10000, 100)
}

func BenchmarkMsgpackResponseWriter_1K(b *testing.B) {
	benchmarkResponseWriter(b, "application/x-msgpack", 10, 100)
}
func BenchmarkMsgpackResponseWriter_100K(b *testing.B) {
	benchmarkResponseWriter(b, "application/x-msgpack", 1000, 100)
}
func BenchmarkMsgpackResponseWriter_1M(b *testing.B) {
	benchmarkResponseWriter(b, "application/x-msgpack", 10000, 100)
}

func BenchmarkCSVResponseWriter_1K(b *testing.B) {
	benchmarkResponseWriter(b, "text/csv", 10, 100)
}
func BenchmarkCSVResponseWriter_100K(b *testing.B) {
	benchmarkResponseWriter(b, "text/csv", 1000, 100)
}
func BenchmarkCSVResponseWriter_1M(b *testing.B) {
	benchmarkResponseWriter(b, "text/csv", 10000, 100)
}

func benchmarkResponseWriter(b *testing.B, contentType string, seriesN, pointsPerSeriesN int) {
	r, err := http.NewRequest("POST", "/query", nil)
	if err != nil {
		b.Fatal(err)
	}
	r.Header.Set("Accept", contentType)

	// Generate a sample result.
	rows := make(models.Rows, 0, seriesN)
	for i := 0; i < seriesN; i++ {
		row := &models.Row{
			Name: "cpu",
			Tags: map[string]string{
				"host": fmt.Sprintf("server-%d", i),
			},
			Columns: []string{"time", "value"},
			Values:  make([][]interface{}, 0, b.N),
		}

		for j := 0; j < pointsPerSeriesN; j++ {
			row.Values = append(row.Values, []interface{}{
				time.Unix(int64(10*j), 0),
				float64(100),
			})
		}
		rows = append(rows, row)
	}
	result := &influxql.Result{Series: rows}

	// Create new ResponseWriter with the underlying ResponseWriter
	// being the discard writer so we only benchmark the marshaling.
	w := httpd.NewResponseWriter(discard{}, r)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		w.WriteResponse(httpd.Response{
			Results: []*influxql.Result{result},
		})
	}
}
