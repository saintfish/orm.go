package orm

import (
	"database/sql"
	"fmt"
	"log"
	"reflect"
	"time"
)

type TableSpec interface {
	Name() string
	Columns() []ColumnSpec
	PrimaryKey() ColumnSpec
	Constraint() string
}

type ColumnSpec interface {
	Name() string
	Type() string
	Constraint() string
	SaveTo(Model) (sql.Scanner, error)
	LoadFrom(Model) (interface{}, error)
}

type Model interface {
	TableSpec() TableSpec
}

type sqlExecutor interface {
	Exec(query string, args ...interface{}) (sql.Result, error)
	Query(query string, args ...interface{}) (*sql.Rows, error)
	QueryRow(query string, args ...interface{}) *sql.Row
}

type Orm struct {
	sqlExecutor
}

func New(db sqlExecutor) *Orm {
	return &Orm{db}
}

func (o *Orm) CreateTable(m Model, ifNotExist bool) error {
	t := reflect.TypeOf(m)
	if t.Kind() != reflect.Ptr {
		panic("Model should be a pointer")
	}
	spec := m.TableSpec()
	data := createTableData{
		Table:           spec.Name(),
		TableConstraint: spec.Constraint(),
		Columns:         spec.Columns(),
		IfNotExist:      ifNotExist,
	}
	sqlStat, err := executeTemplate("createtable", data)
	if err != nil {
		return err
	}
	log.Print(sqlStat)
	_, err = o.Exec(sqlStat)
	return err
}

func (o *Orm) FindByPrimaryKey(m Model) error {
	pk, err := m.TableSpec().PrimaryKey().LoadFrom(m)
	if err != nil {
		return err
	}
	err = o.Select().
		Where(fmt.Sprintf("%s = ?", m.TableSpec().PrimaryKey().Name()), pk).
		Find(m)
	return err
}

func (o *Orm) Select() *selectBuilder {
	return &selectBuilder{orm: o}
}

type selectBuilder struct {
	orm         *Orm
	whereClause string
	whereData   []interface{}
	limitClause string
	orderClause string
}

func (s *selectBuilder) Where(clause string, data ...interface{}) *selectBuilder {
	s.whereClause = clause
	s.whereData = data
	return s
}

func (s *selectBuilder) Limit(clause string) *selectBuilder {
	s.limitClause = clause
	return s
}

func (s *selectBuilder) Order(clause string) *selectBuilder {
	s.orderClause = clause
	return s
}

func (s *selectBuilder) Find(m Model) error {
	t := reflect.TypeOf(m)
	if t.Kind() != reflect.Ptr {
		panic("Model should be a pointer")
	}
	spec := m.TableSpec()
	data := selectData{
		Table:       spec.Name(),
		Columns:     spec.Columns(),
		WhereClause: s.whereClause,
		LimitClause: s.limitClause,
		OrderClause: s.orderClause,
	}
	sqlStat, err := executeTemplate("select", data)
	if err != nil {
		return err
	}
	log.Print(sqlStat, s.whereData)
	row := s.orm.QueryRow(sqlStat, s.whereData...)
	scanDest := make([]interface{}, len(data.Columns))
	for i, c := range data.Columns {
		scanDest[i], err = c.SaveTo(m)
		if err != nil {
			return err
		}
	}
	err = row.Scan(scanDest...)
	return err
}

func (s *selectBuilder) FindAll(ptrOfModelArray interface{}) error {
	t := reflect.TypeOf(ptrOfModelArray)
	if t.Kind() != reflect.Ptr || t.Elem().Kind() != reflect.Slice {
		panic("ptrOfModelArray should be a pointer of a slice")
	}
	var m Model
	t = t.Elem().Elem()
	m = reflect.New(t).Interface().(Model)
	spec := m.TableSpec()
	data := selectData{
		Table:       spec.Name(),
		Columns:     spec.Columns(),
		WhereClause: s.whereClause,
		LimitClause: s.limitClause,
	}
	sqlStat, err := executeTemplate("select", data)
	if err != nil {
		return err
	}
	log.Print(sqlStat, s.whereData)
	rows, err := s.orm.Query(sqlStat, s.whereData...)
	if err != nil {
		return err
	}
	array := reflect.MakeSlice(reflect.SliceOf(t), 0, 10)
	for rows.Next() {
		elem := reflect.New(t)
		scanDest := make([]interface{}, len(data.Columns))
		for i, c := range data.Columns {
			scanDest[i], err = c.SaveTo(elem.Interface().(Model))
			if err != nil {
				return err
			}
		}
		err = rows.Scan(scanDest...)
		if err != nil {
			return err
		}
		array = reflect.Append(array, reflect.Indirect(elem))
	}
	reflect.Indirect(reflect.ValueOf(ptrOfModelArray)).Set(array)
	return nil
}

func (s *selectBuilder) Count(m Model) (int, error) {
	spec := m.TableSpec()
	data := countData{
		Table:       spec.Name(),
		WhereClause: s.whereClause,
	}
	sqlStat, err := executeTemplate("count", data)
	if err != nil {
		return 0, err
	}
	row := s.orm.QueryRow(sqlStat, s.whereData...)
	var count int
	err = row.Scan(&count)
	if err != nil {
		return 0, err
	}
	return count, nil
}

func (o *Orm) Insert(m Model) error {
	spec := m.TableSpec()
	columns := []ColumnSpec{}
	for _, c := range spec.Columns() {
		if spec.PrimaryKey() != nil && c.Name() == spec.PrimaryKey().Name() {
			continue
		}
		columns = append(columns, c)
	}
	data := insertData{
		Table:   spec.Name(),
		Columns: columns,
	}
	sqlStat, err := executeTemplate("insert", data)
	if err != nil {
		return err
	}
	values := []interface{}{}
	for _, c := range data.Columns {
		v, err := c.LoadFrom(m)
		if err != nil {
			return err
		}
		values = append(values, v)
	}
	log.Print(sqlStat, values)
	result, err := o.Exec(sqlStat, values...)
	if err != nil {
		return err
	}
	if spec.PrimaryKey() != nil {
		id, err := result.LastInsertId()
		if err != nil {
			return err
		}
		scanner, err := spec.PrimaryKey().SaveTo(m)
		if err != nil {
			return err
		}
		err = scanner.Scan(id)
		if err != nil {
			return err
		}
	}
	return nil
}

func (o *Orm) Update(m Model, whereClause string, whereData ...interface{}) error {
	spec := m.TableSpec()
	columns := []ColumnSpec{}
	for _, c := range spec.Columns() {
		if spec.PrimaryKey() != nil && c.Name() == spec.PrimaryKey().Name() {
			continue
		}
		columns = append(columns, c)
	}
	data := updateData{
		Table:       spec.Name(),
		Columns:     columns,
		WhereClause: whereClause,
	}
	sqlStat, err := executeTemplate("update", data)
	if err != nil {
		return err
	}
	args := []interface{}{}
	for _, c := range data.Columns {
		col, err := c.LoadFrom(m)
		if err != nil {
			return err
		}
		args = append(args, col)
	}
	args = append(args, whereData...)
	log.Print(sqlStat, args)
	_, err = o.Exec(sqlStat, args...)
	return err
}

func (o *Orm) UpdateByPrimaryKey(m Model) error {
	spec := m.TableSpec()
	pk := spec.PrimaryKey()
	whereClause := fmt.Sprintf("%s = ?", pk.Name())
	whereData, err := pk.LoadFrom(m)
	if err != nil {
		return err
	}
	return o.Update(m, whereClause, whereData)
}

func (o *Orm) Delete(m Model, whereClause string, whereData ...interface{}) error {
	spec := m.TableSpec()
	data := deleteData{
		Table:       spec.Name(),
		WhereClause: whereClause,
	}
	sqlStat, err := executeTemplate("delete", data)
	if err != nil {
		return err
	}
	log.Print(sqlStat, whereData)
	_, err = o.Exec(sqlStat, whereData...)
	return err
}

func (o *Orm) DeleteByPrimaryKey(m Model) error {
	spec := m.TableSpec()
	pk := spec.PrimaryKey()
	whereClause := fmt.Sprintf("%s = ?", pk.Name())
	whereData, err := pk.LoadFrom(m)
	if err != nil {
		return err
	}
	return o.Delete(m, whereClause, whereData)
}

type tableSpec struct {
	table      string
	columns    []ColumnSpec
	pk         ColumnSpec
	constraint string
}

func (spec *tableSpec) Name() string {
	return spec.table
}

func (spec *tableSpec) Columns() []ColumnSpec {
	return spec.columns
}

func (spec *tableSpec) PrimaryKey() ColumnSpec {
	return spec.pk
}

func (spec *tableSpec) Constraint() string {
	return spec.constraint
}

func (spec *tableSpec) hasColumn(name string) bool {
	for _, c := range spec.columns {
		if c.Name() == name {
			return true
		}
	}
	return false
}

func (spec *tableSpec) getColumn(name string) *columnSpec {
	for _, c := range spec.columns {
		if c.Name() == name {
			return c.(*columnSpec)
		}
	}
	return nil
}

func (spec *tableSpec) addColumn(col ColumnSpec) {
	if spec.hasColumn(col.Name()) {
		panic(fmt.Sprintf("Column %s has already been added", col.Name()))
	}
	spec.columns = append(spec.columns, col)
}

func (spec *tableSpec) setPrimaryKey(name string) {
	for _, c := range spec.columns {
		if c.Name() == name {
			spec.pk = c
			spec.pk.(*columnSpec).constraint = "PRIMARY KEY"
			return
		}
	}
	panic(fmt.Sprintf("Invalid column as primary key %s", name))
}

type columnSpec struct {
	column        string
	sqlType       string
	constraint    string
	t             reflect.Type
	saveToModel   func(db interface{}, model interface{}) error
	loadFromModel func(model interface{}) (interface{}, error)
}

type funcScanner func(src interface{}) error

func (f funcScanner) Scan(src interface{}) error {
	return f(src)
}

func (col *columnSpec) Name() string {
	return col.column
}

func (col *columnSpec) Type() string {
	return col.sqlType
}

func (col *columnSpec) Constraint() string {
	return col.constraint
}

func (col *columnSpec) SaveTo(m Model) (sql.Scanner, error) {
	t, err := getIndirectType(m)
	if err != nil {
		return nil, err
	}
	if t != col.t {
		return nil, fmt.Errorf("Type mismatch %v vs %v", t, col.t)
	}
	f := func(src interface{}) error {
		return col.saveToModel(src, m)
	}
	return funcScanner(f), nil
}

func (col *columnSpec) LoadFrom(m Model) (interface{}, error) {
	t, err := getIndirectType(m)
	if err != nil {
		return nil, err
	}
	if t != col.t {
		return nil, fmt.Errorf("Type mismatch %v vs %v", t, col.t)
	}
	return col.loadFromModel(m)
}

type modelField struct {
	fieldIndex []int
}

func (mf *modelField) saveToModel(src, model interface{}) error {
	v := reflect.ValueOf(model)
	if v.Kind() != reflect.Ptr {
		return fmt.Errorf("Model is not a pointer, but %T", model)
	}
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return fmt.Errorf("Model is not a struct pointer, but %T", model)
	}
	v = v.FieldByIndex(mf.fieldIndex)
	if !v.IsValid() {
		return fmt.Errorf("Cannot find field in model %T", model)
	}
	if !v.CanAddr() {
		return fmt.Errorf("Cannot address field in model %T", model)
	}
	v = v.Addr()
	return convertAssign(v.Interface(), src)
}

func (mf *modelField) loadFromModel(model interface{}) (interface{}, error) {
	v := reflect.ValueOf(model)
	if v.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("Model is not a pointer, but %T", model)
	}
	v = reflect.Indirect(v)
	if v.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Model is not a struct pointer, but %T", model)
	}
	v = v.FieldByIndex(mf.fieldIndex)
	if !v.IsValid() {
		return nil, fmt.Errorf("Cannot find field in model %T", model)
	}
	return v.Interface(), nil
}

type structSpecBuilder struct {
	t                reflect.Type
	spec             *tableSpec
	ignoredFields    map[string]bool
	pk               string
	columnConstraint map[string]string
}

func NewStructSpecBuilder(v interface{}) *structSpecBuilder {
	b := &structSpecBuilder{}
	tt := reflect.TypeOf(v)
	if tt.Kind() == reflect.Ptr {
		b.t = tt.Elem()
	} else if tt.Kind() == reflect.Struct {
		b.t = tt
	}
	b.spec = &tableSpec{}
	b.spec.table = b.t.Name()
	return b
}

func (b *structSpecBuilder) SetTable(name string) *structSpecBuilder {
	b.spec.table = name
	return b
}

func getIndirectType(v interface{}) (reflect.Type, error) {
	tt := reflect.TypeOf(v)
	if tt.Kind() != reflect.Ptr {
		return nil, fmt.Errorf("v should be a pointer, but %T", v)
	}
	if tt.Elem().Kind() != reflect.Struct {
		return nil, fmt.Errorf("v should be a struct pointer, but %T", v)
	}
	return tt.Elem(), nil
}

func getExportedField(t reflect.Type, field string) reflect.StructField {
	f, ok := t.FieldByName(field)
	if !ok {
		panic("field " + field + " is not found")
	}
	if len(f.PkgPath) != 0 {
		panic("field " + field + " is unexported")
	}
	return f
}

func getFieldValue(data interface{}, field string) (interface{}, error) {
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Ptr || reflect.Indirect(v).Kind() != reflect.Struct {
		return nil, fmt.Errorf("Data should be a pointer of struct, but %T", data)
	}
	v = reflect.Indirect(v)
	fv := v.FieldByName(field)
	if !fv.IsValid() {
		return nil, fmt.Errorf("Field %s not found in type %T", field, data)
	}
	return fv.Interface(), nil
}

func genericSqlType(t reflect.Type) string {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return "INTEGER"
	case reflect.Float32, reflect.Float64:
		return "REAL"
	case reflect.Bool:
		return "BOOLEAN"
	case reflect.String:
		return "TEXT"
	}
	switch t {
	case reflect.TypeOf([]byte{}):
		return "TEXT"
	case reflect.TypeOf(time.Time{}):
		return "DATETIME"
	}
	panic(fmt.Sprintf("Unsupported field type %v", t))
}

func (b *structSpecBuilder) IgnoreFields(fields ...string) *structSpecBuilder {
	b.ignoredFields = make(map[string]bool)
	for _, name := range fields {
		_ = getExportedField(b.t, name)
		b.ignoredFields[name] = true
	}
	return b
}

func (b *structSpecBuilder) SetColumn(
	name string, typ string, constraint string,
	saveToModel func(db interface{}, model interface{}) error,
	loadFromModel func(model interface{}) (interface{}, error)) *structSpecBuilder {
	b.spec.addColumn(&columnSpec{
		column:        name,
		sqlType:       typ,
		constraint:    constraint,
		t:             b.t,
		saveToModel:   saveToModel,
		loadFromModel: loadFromModel,
	})
	return b
}

func (b *structSpecBuilder) SetFieldColumnType(field, column, typ, constraint string) *structSpecBuilder {
	f := getExportedField(b.t, field)
	mf := &modelField{f.Index}
	return b.SetColumn(column, typ, constraint, mf.saveToModel, mf.loadFromModel)
}

func (b *structSpecBuilder) SetFieldColumn(field, column string) *structSpecBuilder {
	f := getExportedField(b.t, field)
	mf := &modelField{f.Index}
	return b.SetColumn(column, genericSqlType(f.Type), "", mf.saveToModel, mf.loadFromModel)
}

func (b *structSpecBuilder) GenericFields(fields ...string) *structSpecBuilder {
	for _, name := range fields {
		b.SetFieldColumn(name, name)
	}
	return b
}

func (b *structSpecBuilder) GenericOtherFields() *structSpecBuilder {
	genericFields := []string{}
	for i := 0; i < b.t.NumField(); i++ {
		f := b.t.Field(i)
		if len(f.PkgPath) != 0 {
			continue
		}
		if _, ignored := b.ignoredFields[f.Name]; ignored {
			continue
		}
		if b.spec.hasColumn(f.Name) {
			continue
		}
		genericFields = append(genericFields, f.Name)
	}
	return b.GenericFields(genericFields...)
}

func (b *structSpecBuilder) SetPrimaryKey(name string) *structSpecBuilder {
	b.pk = name
	return b
}

func (b *structSpecBuilder) SetConstraints(columnConstraint map[string]string) *structSpecBuilder {
	b.columnConstraint = columnConstraint
	return b
}

func (b *structSpecBuilder) Build() TableSpec {
	if len(b.spec.table) == 0 {
		panic("Empty table name. The data type is unnamed")
	}
	b.spec.setPrimaryKey(b.pk)
	for column, constraint := range b.columnConstraint {
		b.spec.getColumn(column).constraint = constraint
	}
	return b.spec
}
