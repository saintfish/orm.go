package orm

import (
	"bytes"
	"reflect"
	"text/template"
)

var fns = template.FuncMap{
	"last": func(x int, a interface{}) bool {
		return x == reflect.ValueOf(a).Len()-1
	},
}

const createTableTemplate = `CREATE TABLE {{if .IfNotExist}}IF NOT EXISTS {{end}}{{.Table}} ({{range $i, $c := .Columns}}
	{{$c.Name}} {{$c.Type}}{{if $c.Constraint}} {{$c.Constraint}}{{end}}{{if last $i $.Columns | not}},{{end}}{{end}}{{if .TableConstraint}}
	{{.TableConstraint}}{{end}});`

type createTableData struct {
	Table           string
	TableConstraint string
	Columns         []ColumnSpec
	IfNotExist      bool
}

const selectTemplate = `SELECT {{range $i, $c := .Columns}}{{$c.Name}}{{if last $i $.Columns | not}}, {{end}}{{end}}
	FROM {{.Table}}{{if .WhereClause}}
	WHERE {{.WhereClause}}{{end}}{{if .OrderClause}}
	ORDER BY {{.OrderClause}}{{end}}{{if .LimitClause}}
	LIMIT {{.LimitClause}}{{end}};`

type selectData struct {
	Table       string
	Columns     []ColumnSpec
	WhereClause string
	LimitClause string
	OrderClause string
}

const countTemplate = `SELECT count(*)
	FROM {{.Table}}{{if .WhereClause}}
	WHERE {{.WhereClause}}{{end}};`

type countData struct {
	Table       string
	WhereClause string
}

const insertTemplate = `INSERT INTO {{.Table}} ({{range $i, $c := .Columns}}
	{{$c.Name}}{{if last $i $.Columns | not}},{{end}}{{end}}
) VALUES (
	{{range $i, $c := .Columns}}?{{if last $i $.Columns | not}},{{end}}{{end}}
);`

type insertData struct {
	Table   string
	Columns []ColumnSpec
}

const updateTemplate = `UPDATE {{.Table}}
	SET {{range $i, $c := .Columns}}{{$c.Name}} = ?{{if last $i $.Columns | not}}, {{end}}{{end}}
	WHERE {{.WhereClause}};`

type updateData struct {
	Table       string
	Columns     []ColumnSpec
	WhereClause string
}

const deleteTemplate = `DELETE FROM {{.Table}}
	WHERE {{.WhereClause}};`

type deleteData struct {
	Table       string
	WhereClause string
}

var tplMap = map[string]string{
	"createtable": createTableTemplate,
	"select":      selectTemplate,
	"count":       countTemplate,
	"insert":      insertTemplate,
	"update":      updateTemplate,
	"delete":      deleteTemplate,
}

var tpl *template.Template

func init() {
	for k, v := range tplMap {
		if tpl == nil {
			tpl = template.Must(template.New(k).Funcs(fns).Parse(v))
		} else {
			template.Must(tpl.New(k).Parse(v))
		}
	}
}

func executeTemplate(name string, data interface{}) (string, error) {
	b := &bytes.Buffer{}
	err := tpl.ExecuteTemplate(b, name, data)
	if err != nil {
		return "", err
	}
	return b.String(), nil
}
