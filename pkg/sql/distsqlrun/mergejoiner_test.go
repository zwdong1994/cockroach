// Copyright 2016 The Cockroach Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package distsqlrun

import (
	"testing"

	"golang.org/x/net/context"

	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/parser"
	"github.com/cockroachdb/cockroach/pkg/sql/sqlbase"
	"github.com/cockroachdb/cockroach/pkg/util/encoding"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
)

func TestMergeJoiner(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i)))
	}
	null := sqlbase.EncDatum{Datum: parser.DNull}

	testCases := []struct {
		spec     MergeJoinerSpec
		outCols  []uint32
		inputs   []sqlbase.EncDatumRows
		expected sqlbase.EncDatumRows
	}{
		{
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: JoinType_INNER,
				// Implicit @1 = @3 constraint.
			},
			outCols: []uint32{0, 3, 4},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], v[4]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
					{v[5], v[5]},
				},
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[1], v[0], v[4]},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
			},
		},
		{
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: JoinType_INNER,
				// Implicit @1 = @3 constraint.
			},
			outCols: []uint32{0, 1, 3},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[0], v[1]},
				},
				{
					{v[0], v[4]},
					{v[0], v[1]},
					{v[0], v[0]},
					{v[0], v[5]},
					{v[0], v[4]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[0], v[0], v[1]},
				{v[0], v[0], v[0]},
				{v[0], v[0], v[5]},
				{v[0], v[0], v[4]},
				{v[0], v[1], v[4]},
				{v[0], v[1], v[1]},
				{v[0], v[1], v[0]},
				{v[0], v[1], v[5]},
				{v[0], v[1], v[4]},
			},
		},
		{
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:   JoinType_INNER,
				OnExpr: Expression{Expr: "@4 >= 4"},
				// Implicit AND @1 = @3 constraint.
			},
			outCols: []uint32{0, 1, 3},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[0], v[1]},
					{v[1], v[0]},
					{v[1], v[1]},
				},
				{
					{v[0], v[4]},
					{v[0], v[1]},
					{v[0], v[0]},
					{v[0], v[5]},
					{v[0], v[4]},
					{v[1], v[4]},
					{v[1], v[1]},
					{v[1], v[0]},
					{v[1], v[5]},
					{v[1], v[4]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], v[4]},
				{v[0], v[0], v[5]},
				{v[0], v[0], v[4]},
				{v[0], v[1], v[4]},
				{v[0], v[1], v[5]},
				{v[0], v[1], v[4]},
				{v[1], v[0], v[4]},
				{v[1], v[0], v[5]},
				{v[1], v[0], v[4]},
				{v[1], v[1], v[4]},
				{v[1], v[1], v[5]},
				{v[1], v[1], v[4]},
			},
		},
		{
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type:   JoinType_FULL_OUTER,
				OnExpr: Expression{Expr: "@2 >= @4"},
				// Implicit AND @1 = @3 constraint.
			},
			outCols: []uint32{0, 1, 3},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[0], v[0]},

					{v[1], v[5]},

					{v[2], v[0]},
					{v[2], v[8]},

					{v[3], v[5]},

					{v[6], v[0]},
				},
				{
					{v[0], v[5]},
					{v[0], v[5]},

					{v[1], v[0]},
					{v[1], v[8]},

					{v[2], v[5]},

					{v[3], v[0]},
					{v[3], v[0]},

					{v[5], v[0]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], v[0], null},
				{v[0], v[0], null},
				{null, null, v[5]},
				{null, null, v[5]},

				{v[1], v[5], v[0]},
				{null, null, v[8]},

				{v[2], v[0], null},
				{v[2], v[8], v[5]},

				{v[3], v[5], v[0]},
				{v[3], v[5], v[0]},

				{null, null, v[0]},

				{v[6], v[0], null},
			},
		},
		{
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: JoinType_LEFT_OUTER,
				// Implicit @1 = @3 constraint.
			},
			outCols: []uint32{0, 3, 4},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], v[4]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
					{v[5], v[5]},
				},
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], null, null},
				{v[1], v[0], v[4]},
				{v[2], null, null},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
				{v[5], null, null},
			},
		},
		{
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: JoinType_RIGHT_OUTER,
				// Implicit @1 = @3 constraint.
			},
			outCols: []uint32{3, 1, 2},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
				},
				{
					{v[0], v[0]},
					{v[1], v[4]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
					{v[5], v[5]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], null, null},
				{v[1], v[0], v[4]},
				{v[2], null, null},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
				{v[5], null, null},
			},
		},
		{
			spec: MergeJoinerSpec{
				LeftOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				RightOrdering: convertToSpecOrdering(
					sqlbase.ColumnOrdering{
						{ColIdx: 0, Direction: encoding.Ascending},
					}),
				Type: JoinType_FULL_OUTER,
				// Implicit @1 = @3 constraint.
			},
			outCols: []uint32{0, 3, 4},
			inputs: []sqlbase.EncDatumRows{
				{
					{v[0], v[0]},
					{v[1], v[4]},
					{v[2], v[4]},
					{v[3], v[1]},
					{v[4], v[5]},
				},
				{
					{v[1], v[0], v[4]},
					{v[3], v[4], v[1]},
					{v[4], v[4], v[5]},
					{v[5], v[5], v[1]},
				},
			},
			expected: sqlbase.EncDatumRows{
				{v[0], null, null},
				{v[1], v[0], v[4]},
				{v[2], null, null},
				{v[3], v[4], v[1]},
				{v[4], v[4], v[5]},
				{null, v[5], v[1]},
			},
		},
	}

	for _, c := range testCases {
		t.Run("", func(t *testing.T) {
			ms := c.spec
			leftInput := NewRowBuffer(nil /* types */, c.inputs[0], RowBufferArgs{})
			rightInput := NewRowBuffer(nil /* types */, c.inputs[1], RowBufferArgs{})
			out := &RowBuffer{}
			evalCtx := parser.MakeTestingEvalContext()
			defer evalCtx.Stop(context.Background())
			flowCtx := FlowCtx{Settings: cluster.MakeTestingClusterSettings(), EvalCtx: evalCtx}

			post := PostProcessSpec{Projection: true, OutputColumns: c.outCols}
			m, err := newMergeJoiner(&flowCtx, &ms, leftInput, rightInput, &post, out)
			if err != nil {
				t.Fatal(err)
			}

			m.Run(context.Background(), nil /* wg */)

			if !out.ProducerClosed {
				t.Fatalf("output RowReceiver not closed")
			}

			var retRows sqlbase.EncDatumRows
			for {
				row, meta := out.Next()
				if err != nil {
					t.Fatal(err)
				}
				if !meta.Empty() {
					t.Fatalf("unexpected metadata: %v", meta)
				}
				if row == nil {
					break
				}
				retRows = append(retRows, row)
			}
			expStr := c.expected.String()
			retStr := retRows.String()
			if expStr != retStr {
				t.Errorf("invalid results; expected:\n   %s\ngot:\n   %s",
					expStr, retStr)
			}
		})
	}
}

// Test that the joiner shuts down fine if the consumer is closed prematurely.
func TestConsumerClosed(t *testing.T) {
	defer leaktest.AfterTest(t)()

	columnTypeInt := sqlbase.ColumnType{SemanticType: sqlbase.ColumnType_INT}
	v := [10]sqlbase.EncDatum{}
	for i := range v {
		v[i] = sqlbase.DatumToEncDatum(columnTypeInt, parser.NewDInt(parser.DInt(i)))
	}

	spec := MergeJoinerSpec{
		LeftOrdering: convertToSpecOrdering(
			sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		RightOrdering: convertToSpecOrdering(
			sqlbase.ColumnOrdering{
				{ColIdx: 0, Direction: encoding.Ascending},
			}),
		Type: JoinType_INNER,
		// Implicit @1 = @2 constraint.
	}
	outCols := []uint32{0}
	leftTypes := []sqlbase.ColumnType{{SemanticType: sqlbase.ColumnType_INT}}
	rightTypes := []sqlbase.ColumnType{{SemanticType: sqlbase.ColumnType_INT}}

	testCases := []struct {
		typ       JoinType
		leftRows  sqlbase.EncDatumRows
		rightRows sqlbase.EncDatumRows
	}{
		{
			typ: JoinType_INNER,
			// Implicit @1 = @2 constraint.
			leftRows: sqlbase.EncDatumRows{
				{v[0]},
			},
			rightRows: sqlbase.EncDatumRows{
				{v[0]},
			},
		},
		{
			typ: JoinType_LEFT_OUTER,
			// Implicit @1 = @2 constraint.
			leftRows: sqlbase.EncDatumRows{
				{v[0]},
			},
			rightRows: sqlbase.EncDatumRows{},
		},
		{
			typ: JoinType_RIGHT_OUTER,
			// Implicit @1 = @2 constraint.
			leftRows: sqlbase.EncDatumRows{},
			rightRows: sqlbase.EncDatumRows{
				{v[0]},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.typ.String() /* name */, func(t *testing.T) {
			leftInput := NewRowBuffer(leftTypes, tc.leftRows, RowBufferArgs{})
			rightInput := NewRowBuffer(rightTypes, tc.rightRows, RowBufferArgs{})

			// Create a consumer and close it immediately. The mergeJoiner should find out
			// about this closer the first time it attempts to push a row.
			out := &RowBuffer{}
			out.ConsumerDone()

			evalCtx := parser.MakeTestingEvalContext()
			defer evalCtx.Stop(context.Background())
			flowCtx := FlowCtx{
				Settings: cluster.MakeTestingClusterSettings(),
				EvalCtx:  evalCtx,
			}
			post := PostProcessSpec{Projection: true, OutputColumns: outCols}
			m, err := newMergeJoiner(&flowCtx, &spec, leftInput, rightInput, &post, out)
			if err != nil {
				t.Fatal(err)
			}

			m.Run(context.TODO(), nil /* wg */)

			if !out.ProducerClosed {
				t.Fatalf("output RowReceiver not closed")
			}
		})
	}
}
