package pgtenant

import (
	"bytes"
	"fmt"
	"io"
	"reflect"
	"sort"
	"strings"

	pg_query "github.com/lfittl/pg_query_go"
	nodes "github.com/lfittl/pg_query_go/nodes"
	"github.com/lib/pq"
	"github.com/pkg/errors"
)

type status int

const (
	noStatus status = iota
	needsTenantID
	hasTenantID
	isCTE
	isLeftJoinTable
)

type environ map[string]status

func newEnv() environ {
	return make(map[string]status)
}

type transformer struct {
	*Conn
	tenantIDNum   int  // number of the added positional parameter for the tenant ID value
	isTransformed bool // whether a tenant ID arg was added
}

func (t *transformer) transformTree(tree pg_query.ParsetreeList) (string, error) {
	if len(tree.Statements) != 1 {
		return "", fmt.Errorf("%d statements in parse tree, want 1", len(tree.Statements))
	}
	return t.transformStmt(tree.Statements[0])
}

func (t *transformer) transformStmt(stmt nodes.Node) (string, error) {
	if raw, ok := stmt.(nodes.RawStmt); ok {
		return t.transformStmt(raw.Stmt)
	}

	var (
		buf = new(bytes.Buffer)
		env = newEnv()
		err error
	)

	switch stmt := stmt.(type) {
	case nodes.InsertStmt:
		err = t.transformInsert(buf, stmt, env)
	case nodes.SelectStmt:
		err = t.transformSelect(buf, stmt, env, nil)
	case nodes.UpdateStmt:
		err = t.transformUpdate(buf, stmt, env)
	case nodes.DeleteStmt:
		err = t.transformDelete(buf, stmt, env)
	default:
		return "", fmt.Errorf("unknown statement type %T", stmt)
	}

	return buf.String(), err
}

func (t *transformer) transformInsert(w io.Writer, stmt nodes.InsertStmt, env environ) error {
	cteNames, err := t.handleCTE(w, stmt.WithClause, env)
	if err != nil {
		return errors.Wrap(err, "transformInsert")
	}
	fmt.Fprintf(w, "INSERT INTO %s ", safestr(*stmt.Relation.Relname))
	if stmt.Relation.Alias != nil {
		fmt.Fprintf(w, "AS %s ", safestr(*stmt.Relation.Alias.Aliasname))
		env[*stmt.Relation.Alias.Aliasname] = needsTenantID
	} else {
		env[*stmt.Relation.Relname] = needsTenantID
	}
	fmt.Fprint(w, "(")
	for _, col := range stmt.Cols.Items {
		name, ok := col.(nodes.ResTarget)
		if !ok {
			return fmt.Errorf("INSERT INTO (...) item is a %T, want ResTarget", col)
		}
		fmt.Fprintf(w, "%s, ", safestr(*name.Name))
	}
	fmt.Fprintf(w, "%s) ", t.driver.TenantIDCol)
	sel, ok := stmt.SelectStmt.(nodes.SelectStmt)
	if !ok {
		return fmt.Errorf("INSERT select statement is a %T, want SelectStmt", stmt.SelectStmt)
	}
	switch len(sel.ValuesLists) {
	case 0:
		// INSERT ... SELECT
		subEnv := newEnv()
		for _, cteName := range cteNames {
			subEnv[cteName] = isCTE
		}
		err := t.transformSelect(w, sel, subEnv, &stmt)
		if err != nil {
			return errors.Wrap(err, "transformInsert")
		}

	case 1:
		// INSERT ... VALUES
		fmt.Fprint(w, "VALUES (")
		for _, node := range sel.ValuesLists[0] {
			err := t.transformNode(w, node, env)
			if err != nil {
				return errors.Wrap(err, "transformInsert")
			}
			fmt.Fprint(w, ", ")
		}
		t.addTenantID(w)
		io.WriteString(w, ")")

	default:
		return fmt.Errorf("SELECT statement has %d items in ValuesLists, want 0 or 1", len(sel.ValuesLists))
	}

	if stmt.OnConflictClause != nil && stmt.OnConflictClause.Action != nodes.ONCONFLICT_NONE {
		fmt.Fprint(w, " ON CONFLICT ")
		if stmt.OnConflictClause.Infer != nil {
			fmt.Fprint(w, "(")
			for _, node := range stmt.OnConflictClause.Infer.IndexElems.Items {
				elem, ok := node.(nodes.IndexElem)
				if !ok {
					return fmt.Errorf("ON CONFLICT index element is a %T, want IndexElem", node)
				}
				err := t.transformIndexElem(w, elem)
				if err != nil {
					return errors.Wrap(err, "transformInsert")
				}
				fmt.Fprint(w, ", ")
			}
			fmt.Fprintf(w, "%s) ", t.driver.TenantIDCol)
		}
		fmt.Fprint(w, "DO ")
		switch stmt.OnConflictClause.Action {
		case nodes.ONCONFLICT_NOTHING:
			fmt.Fprint(w, "NOTHING")
		case nodes.ONCONFLICT_UPDATE:
			fmt.Fprint(w, "UPDATE SET ")
			err := commaSeparated(w, stmt.OnConflictClause.TargetList.Items, env, t.transformNode)
			if err != nil {
				return errors.Wrap(err, "transformInsert")
			}
			if stmt.OnConflictClause.WhereClause != nil {
				err = t.transformWhere(w, stmt.OnConflictClause.WhereClause, env, true)
				if err != nil {
					return errors.Wrap(err, "transformInsert")
				}
			}
		}
	}

	if len(stmt.ReturningList.Items) > 0 {
		fmt.Fprint(w, " RETURNING ")
		err := commaSeparated(w, stmt.ReturningList.Items, env, t.transformNode)
		if err != nil {
			return errors.Wrap(err, "transformInsert")
		}
	}

	return nil
}

func (t *transformer) transformSelect(w io.Writer, stmt nodes.SelectStmt, env environ, insertStmt *nodes.InsertStmt) error {
	_, err := t.handleCTE(w, stmt.WithClause, env)
	if err != nil {
		return errors.Wrap(err, "transformSelect")
	}
	fmt.Fprint(w, "SELECT ")
	if len(stmt.DistinctClause.Items) > 0 {
		return fmt.Errorf("SELECT DISTINCT not implemented")
	}
	targetItems := stmt.TargetList.Items
	var star bool
	if len(targetItems) == 1 {
		targetItem, ok := targetItems[0].(nodes.ResTarget)
		if !ok {
			return fmt.Errorf("SELECT target item is a %T, want ResTarget", targetItem)
		}
		if colRef, ok := targetItem.Val.(nodes.ColumnRef); ok {
			resTargetItems := colRef.Fields.Items
			if len(resTargetItems) == 1 {
				if _, ok := resTargetItems[0].(nodes.A_Star); ok {
					fmt.Fprint(w, "*")
					if insertStmt != nil {
						return fmt.Errorf("cannot handle INSERT ... SELECT *")
					}
					star = true
				}
			}
		}
	}
	if !star {
		err := commaSeparated(w, targetItems, env, t.transformSelectCol)
		if err != nil {
			return errors.Wrap(err, "transformSelect")
		}
		if insertStmt != nil {
			if len(targetItems) > 0 {
				fmt.Fprint(w, ", ")
			}
			t.addTenantID(w)
		}
	}
	fromItems := stmt.FromClause.Items
	if len(fromItems) > 0 {
		fmt.Fprint(w, " FROM ")
		err := commaSeparated(w, fromItems, env, t.transformNode)
		if err != nil {
			return errors.Wrap(err, "transformSelect")
		}
	}
	err = t.transformWhere(w, stmt.WhereClause, env, false)
	if err != nil {
		return errors.Wrap(err, "transformSelect")
	}
	if len(stmt.GroupClause.Items) > 0 {
		fmt.Fprint(w, " GROUP BY ")
		err := commaSeparated(w, stmt.GroupClause.Items, env, t.transformNode)
		if err != nil {
			return errors.Wrap(err, "transformSelect")
		}
	}
	if stmt.HavingClause != nil {
		fmt.Fprint(w, " HAVING ")
		err = t.transformNode(w, stmt.HavingClause, env)
		if err != nil {
			return errors.Wrap(err, "transformSelect")
		}
	}
	if len(stmt.SortClause.Items) > 0 {
		fmt.Fprint(w, " ORDER BY ")
		err := commaSeparated(w, stmt.SortClause.Items, env, func(w io.Writer, item nodes.Node, env environ) error {
			sortBy, ok := item.(nodes.SortBy)
			if !ok {
				return fmt.Errorf("SORT BY clause is a %T, want SortBy", item)
			}
			err := t.transformNode(w, sortBy.Node, env)
			if err != nil {
				return err
			}
			switch sortBy.SortbyDir {
			case nodes.SORTBY_ASC:
				fmt.Fprint(w, " ASC")
			case nodes.SORTBY_DESC:
				fmt.Fprint(w, " DESC")
			}
			return nil
		})
		if err != nil {
			return errors.Wrap(err, "transformSelect")
		}
	}
	if stmt.LimitCount != nil {
		fmt.Fprint(w, " LIMIT ")
		err = t.transformNode(w, stmt.LimitCount, env)
		if err != nil {
			return errors.Wrap(err, "transformSelect")
		}
	}
	return nil
}

func (t *transformer) transformWhere(w io.Writer, where nodes.Node, env environ, onConflict bool) error {
	// If this query involves a LEFT JOIN, the main table that we do the LEFT JOIN on would have
	// a status "isLeftJoinTable". We will have to add the tenant ID in the where clause so that
	// the LEFT JOIN won't include other tenants' data.
	for tbl, status := range env {
		if status == isLeftJoinTable {
			env[tbl] = needsTenantID
			break
		}
	}
	doWhere := where != nil
	if !doWhere {
		for _, state := range env {
			if state == needsTenantID {
				doWhere = true
				break
			}
		}
	}
	if !doWhere {
		return nil
	}
	fmt.Fprint(w, " WHERE ")
	var tables sort.StringSlice
	for table := range env {
		tables = append(tables, table)
	}
	tables.Sort()
	return t.transformWhereHelper(w, where, env, onConflict, tables)
}

func (t *transformer) transformWhereHelper(w io.Writer, where nodes.Node, env environ, onConflict bool, tables []string) error {
	var addTenantID bool
	for _, table := range tables {
		if env[table] == needsTenantID {
			addTenantID = true
			break
		}
	}
	if !addTenantID {
		return t.transformNode(w, where, env)
	}
	if where != nil {
		if boolexpr, ok := where.(nodes.BoolExpr); ok && boolexpr.Boolop != nodes.AND_EXPR {
			fmt.Fprint(w, "(")
			err := t.transformBoolExpr(w, boolexpr, env)
			if err != nil {
				return errors.Wrap(err, "transformWhere")
			}
			fmt.Fprint(w, ")")
		} else {
			err := t.transformNode(w, where, env)
			if err != nil {
				return errors.Wrap(err, "transformWhere")
			}
		}
		fmt.Fprint(w, " AND ")
	}

	first := true
	for _, table := range tables {
		if env[table] != needsTenantID {
			continue
		}
		if first {
			first = false
		} else {
			fmt.Fprint(w, " AND ")
		}
		if len(tables) > 1 || onConflict {
			fmt.Fprint(w, table, ".")
		}
		fmt.Fprint(w, t.driver.TenantIDCol, " = ")
		t.addTenantID(w)
		env[table] = hasTenantID
	}
	return nil
}

func extractTables(from []nodes.Node) (map[string]bool, error) {
	m := make(map[string]bool)
	for _, f := range from {
		extractTablesAux(f, m)
	}
	return m, nil
}

func extractTablesAux(n nodes.Node, out map[string]bool) {
	switch n := n.(type) {
	case nodes.RangeVar:
		if n.Alias != nil {
			out[*n.Alias.Aliasname] = true
		} else {
			out[*n.Relname] = true
		}

	case nodes.JoinExpr:
		extractTablesAux(n.Larg, out)
		extractTablesAux(n.Rarg, out)

	case nodes.RangeSubselect:
		if n.Alias != nil && n.Alias.Aliasname != nil && *n.Alias.Aliasname != "" {
			out[*n.Alias.Aliasname] = true
		}
	}
}

func (t *transformer) transformUpdate(w io.Writer, stmt nodes.UpdateStmt, env environ) error {
	_, err := t.handleCTE(w, stmt.WithClause, env)
	if err != nil {
		return errors.Wrap(err, "transformUpdate")
	}
	fmt.Fprint(w, "UPDATE ")
	err = t.transformNode(w, *stmt.Relation, env)
	if err != nil {
		return errors.Wrap(err, "transformUpdate")
	}
	fmt.Fprint(w, " SET ")
	err = commaSeparated(w, stmt.TargetList.Items, env, t.transformNode)
	if err != nil {
		return errors.Wrap(err, "transformUpdate")
	}
	if len(stmt.FromClause.Items) > 0 {
		fmt.Fprint(w, " FROM ")
		err = commaSeparated(w, stmt.FromClause.Items, env, t.transformNode)
		if err != nil {
			return errors.Wrap(err, "transformUpdate")
		}
	}
	if stmt.WhereClause == nil {
		fmt.Fprintf(w, " WHERE %s = ", t.driver.TenantIDCol)
		t.addTenantID(w)
		return nil
	}
	err = t.transformWhere(w, stmt.WhereClause, env, false)
	if err != nil {
		return errors.Wrap(err, "transformUpdate")
	}
	if len(stmt.ReturningList.Items) > 0 {
		fmt.Fprint(w, " RETURNING ")
		err = commaSeparated(w, stmt.ReturningList.Items, env, t.transformNode)
		if err != nil {
			return errors.Wrap(err, "transformUpdate")
		}
	}
	return nil
}

// returns the list of cte aliases
func (t *transformer) handleCTE(w io.Writer, withClause *nodes.WithClause, env environ) ([]string, error) {
	if withClause == nil {
		return nil, nil
	}

	var cteNames []string

	fmt.Fprint(w, "WITH ")
	if withClause.Recursive {
		fmt.Fprint(w, "RECURSIVE ")
	}

	for i, cteItem := range withClause.Ctes.Items {
		if i > 0 {
			fmt.Fprint(w, ", ")
		}

		cte, ok := cteItem.(nodes.CommonTableExpr)
		if !ok {
			return nil, fmt.Errorf("Ctequery item is a %T, want CommonTableExpr", cteItem)
		}
		fmt.Fprintf(w, "%s AS (", safestr(*cte.Ctename))
		env[*cte.Ctename] = isCTE

		cteNames = append(cteNames, *cte.Ctename)

		switch substmt := cte.Ctequery.(type) {
		case nodes.SelectStmt:
			subEnv := newEnv()
			err := t.transformSelect(w, substmt, subEnv, nil)
			if err != nil {
				return nil, errors.Wrap(err, "handleCTE")
			}
			for tbl, state := range subEnv {
				if state == isCTE {
					env[tbl] = isCTE
				}
			}

		case nodes.InsertStmt:
			if substmt.SelectStmt == nil {
				return nil, fmt.Errorf("Ctequery has no SELECT")
			}
			subEnv := newEnv()
			err := t.transformInsert(w, substmt, subEnv)
			if err != nil {
				return nil, errors.Wrap(err, "handleCTE")
			}
			for tbl, state := range subEnv {
				if state == isCTE {
					env[tbl] = isCTE
				}
			}

		default:
			return nil, fmt.Errorf("Ctequery is a %T, want SELECT or INSERT ... SELECT", cte.Ctequery)
		}

		fmt.Fprint(w, ")")
	}

	fmt.Fprint(w, " ")
	return cteNames, nil
}

func (t *transformer) transformDelete(w io.Writer, stmt nodes.DeleteStmt, env environ) error {
	fmt.Fprint(w, "DELETE FROM ")
	err := t.transformNode(w, *stmt.Relation, env)
	if err != nil {
		return errors.Wrap(err, "transformDelete")
	}

	if len(stmt.UsingClause.Items) > 0 {
		fmt.Fprint(w, " USING ")
		err = commaSeparated(w, stmt.UsingClause.Items, env, t.transformNode)
		if err != nil {
			return errors.Wrap(err, "transformDelete")
		}
	}

	return t.transformWhere(w, stmt.WhereClause, env, false)
}

func (t *transformer) transformSelectCol(w io.Writer, node nodes.Node, env environ) error {
	target, ok := node.(nodes.ResTarget)
	if !ok {
		return fmt.Errorf("transformSelectCol: got %T, want ResTarget", node)
	}
	err := t.transformNode(w, target.Val, env)
	if err != nil {
		return errors.Wrap(err, "transformSelectCol")
	}
	if target.Name != nil {
		fmt.Fprintf(w, " AS %s", safestr(*target.Name))
	}
	return nil
}

// like transformNode but parenthesizes its output if it's not an atom
func (t *transformer) transformAtom(w io.Writer, node nodes.Node, env environ) error {
	buf := new(bytes.Buffer)

	isAtomic, err := t.transformNodeHelper(buf, node, env)
	if err != nil {
		return err
	}
	if isAtomic {
		w.Write(buf.Bytes())
	} else {
		fmt.Fprintf(w, "(%s)", buf.String())
	}
	return nil
}

func (t *transformer) transformNode(w io.Writer, node nodes.Node, env environ) error {
	_, err := t.transformNodeHelper(w, node, env)
	return err
}

// Returns true if the emitted node is "atomic" - i.e., doesn't need
// extra parens.  Identifiers and literals are atomic; binary
// expressions [A+B] are not; function calls [A(B, C, ...)] are; etc.
func (t *transformer) transformNodeHelper(w io.Writer, node nodes.Node, env environ) (bool, error) {
	switch node := node.(type) {
	case nodes.RangeSubselect:
		subquery, ok := node.Subquery.(nodes.SelectStmt)
		if !ok {
			return false, fmt.Errorf("RangeSubselect with %T subquery not handled", node.Subquery)
		}

		hasAlias := node.Alias != nil && node.Alias.Aliasname != nil && *node.Alias.Aliasname != ""
		if hasAlias {
			fmt.Fprint(w, "(")
		}
		err := t.transformSelect(w, subquery, newEnv(), nil)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (RangeSubselect)")
		}
		if hasAlias {
			fmt.Fprintf(w, ") AS %s", *node.Alias.Aliasname)
			env[*node.Alias.Aliasname] = isCTE
		}
		return false, nil

	case nodes.ParamRef:
		fmt.Fprintf(w, "$%d", node.Number)
		return true, nil

	case nodes.TypeCast:
		if t.specialCaseBoolLiteral(w, node) {
			return true, nil
		}
		err := t.transformAtom(w, node.Arg, env)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (TypeCast)")
		}
		fmt.Fprint(w, "::")
		err = t.transformTypeName(w, *node.TypeName)
		return false, err

	case nodes.ResTarget:
		if node.Name != nil {
			fmt.Fprint(w, safestr(*node.Name))
		}
		if node.Name != nil && node.Val != nil {
			fmt.Fprint(w, " = ")
		}
		if node.Val != nil {
			err := t.transformNode(w, node.Val, env)
			if err != nil {
				return false, errors.Wrap(err, "transformNode (ResTarget)")
			}
		}
		return node.Val == nil, nil

	case nodes.ColumnRef:
		if len(node.Fields.Items) == 0 {
			return false, fmt.Errorf("no fields in ColumnRef node")
		}
		err := t.transformNodeSafe(w, node.Fields.Items[0], env)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (ColumnRef)")
		}
		if len(node.Fields.Items) > 1 {
			fmt.Fprint(w, ".")
			if _, ok := node.Fields.Items[1].(nodes.A_Star); ok {
				fmt.Fprint(w, "*")
				return false, nil
			}
			err = t.transformNodeSafe(w, node.Fields.Items[1], env)
			if err != nil {
				return false, errors.Wrap(err, "transformNode (ColumnRef)")
			}
			return false, nil
		}
		return true, nil

	case nodes.String:
		fmt.Fprint(w, node.Str)
		return true, nil

	case nodes.A_Expr:
		switch node.Kind {
		case nodes.AEXPR_OP: // normal operator
			err := t.transformNode(w, node.Lexpr, env)
			if err != nil {
				return false, errors.Wrap(err, "transformNode (A_Expr/OP)")
			}
			if len(node.Name.Items) != 1 {
				return false, fmt.Errorf("%d names for A_Expr operator, want 1", len(node.Name.Items))
			}
			op, ok := node.Name.Items[0].(nodes.String)
			if !ok {
				return false, fmt.Errorf("name for A_Expr operator is a %T, want Str", node.Name.Items[0])
			}
			switch op.Str {
			case "->", "->>":
				fmt.Fprint(w, op.Str)
			default:
				fmt.Fprintf(w, " %s ", op.Str)
			}
			err = t.transformNode(w, node.Rexpr, env)
			if err != nil {
				return false, errors.Wrap(err, "transformNode (A_Expr/OP)")
			}

		case nodes.AEXPR_OP_ANY: // scalar op ANY (array)
			err := t.transformNode(w, node.Lexpr, env)
			if err != nil {
				return false, errors.Wrap(err, "transformNode (A_Expr/OP_ANY")
			}
			if len(node.Name.Items) != 1 {
				return false, fmt.Errorf("%d names for A_Expr operator, want 1", len(node.Name.Items))
			}
			op, ok := node.Name.Items[0].(nodes.String)
			if !ok {
				return false, fmt.Errorf("name for A_Expr operator is a %T, want Str", node.Name.Items[0])
			}
			fmt.Fprintf(w, " %s ANY(", op.Str)
			err = t.transformNode(w, node.Rexpr, env)
			if err != nil {
				return false, errors.Wrap(err, "transformNode (A_Expr/OP_ANY")
			}
			fmt.Fprint(w, ")")

		case nodes.AEXPR_NULLIF: // NULLIF(left, right)
			fmt.Fprint(w, "NULLIF(")
			err := t.transformNode(w, node.Lexpr, env)
			if err != nil {
				return false, errors.Wrap(err, "transformNode(A_Expr/NULLIF left subexp)")
			}
			fmt.Fprint(w, ", ")
			err = t.transformNode(w, node.Rexpr, env)
			if err != nil {
				return false, errors.Wrap(err, "transformNode(A_Expr/NULLIF right subexp)")
			}
			fmt.Fprint(w, ")")

		default:
			return false, fmt.Errorf("A_Expr subtype %v not implemented", node.Kind)

			// case nodes.AEXPR_OP_ALL: // scalar op ALL (array)
			// case nodes.AEXPR_DISTINCT: // IS DISTINCT FROM - name must be "="
			// case nodes.AEXPR_NULLIF: // NULLIF - name must be "="
			// case nodes.AEXPR_OF: // IS [NOT] OF - name must be "=" or "<>"
			// case nodes.AEXPR_IN: // [NOT] IN - name must be "=" or "<>"
			// case nodes.AEXPR_LIKE: // [NOT] LIKE - name must be "~~" or "!~~"
			// case nodes.AEXPR_ILIKE: // [NOT] ILIKE - name must be "~~*" or "!~~*"
			// case nodes.AEXPR_SIMILAR: // [NOT] SIMILAR - name must be "~" or "!~"
			// case nodes.AEXPR_BETWEEN: // name must be "BETWEEN"
			// case nodes.AEXPR_NOT_BETWEEN: // name must be "NOT BETWEEN"
			// case nodes.AEXPR_BETWEEN_SYM: // name must be "BETWEEN SYMMETRIC"
			// case nodes.AEXPR_NOT_BETWEEN_SYM: // name must be "NOT BETWEEN SYMMETRIC"
			// case nodes.AEXPR_PAREN: // nameless dummy node for parentheses
		}
		return false, nil

	case nodes.FuncCall:
		err := t.transformIdent(w, node.Funcname)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (FuncCall)")
		}
		fmt.Fprint(w, "(")
		if len(node.Args.Items) == 0 && node.AggStar {
			fmt.Fprint(w, "*")
		} else {
			err = commaSeparated(w, node.Args.Items, env, t.transformNode)
			if err != nil {
				return false, errors.Wrap(err, "transformNode (FuncCall)")
			}
		}
		fmt.Fprint(w, ")")
		return true, nil

	case nodes.BoolExpr:
		return false, t.transformBoolExpr(w, node, env)

	case nodes.SubLink:
		return false, t.transformSubLink(w, node, env)

	case nodes.A_Const:
		return true, t.transformConst(w, node)

	case nodes.RangeVar:
		fmt.Fprint(w, safestr(*node.Relname))
		if node.Alias != nil {
			fmt.Fprintf(w, " %s", safestr(*node.Alias.Aliasname))
			if env != nil && env[*node.Alias.Aliasname] == noStatus {
				st := env[*node.Relname]
				if st == noStatus {
					st = needsTenantID
				}
				env[*node.Alias.Aliasname] = st
			}
			return false, nil
		}
		if env != nil && env[*node.Relname] == noStatus {
			env[*node.Relname] = needsTenantID
		}
		return true, nil

	case nodes.CoalesceExpr:
		fmt.Fprint(w, "COALESCE(")
		err := commaSeparated(w, node.Args.Items, env, t.transformNode)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (CoalesceExpr)")
		}
		fmt.Fprint(w, ")")
		return true, nil

	case nodes.SetToDefault:
		fmt.Fprint(w, "DEFAULT")
		return true, nil

	case nodes.NullTest:
		err := t.transformNode(w, node.Arg, env)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (NullTest)")
		}
		switch node.Nulltesttype {
		case nodes.IS_NULL:
			fmt.Fprint(w, " IS NULL")
		case nodes.IS_NOT_NULL:
			fmt.Fprint(w, " IS NOT NULL")
		}
		return false, nil

	case nodes.CaseExpr:
		if node.Arg != nil {
			return false, errors.New("CASE testexpr WHEN ... not implemented")
		}
		fmt.Fprint(w, "CASE ")
		for _, arg := range node.Args.Items {
			err := t.transformNode(w, arg, env)
			if err != nil {
				return false, errors.Wrap(err, "transformNode (CaseExpr Args)")
			}
		}
		fmt.Fprint(w, " ELSE ")
		err := t.transformNode(w, node.Defresult, env)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (CaseExpr Defresult)")
		}
		fmt.Fprint(w, " END")
		return true, nil

	case nodes.CaseWhen:
		fmt.Fprint(w, "WHEN ")
		err := t.transformNode(w, node.Expr, env)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (CaseWhen)")
		}
		fmt.Fprint(w, " THEN ")
		err = t.transformNode(w, node.Result, env)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (CaseWhen)")
		}
		return true, nil

	case nodes.JoinExpr:
		// Set the status of the main table that the query performs LEFT JOIN on to "isLeftJoinTable"
		// because we don't want to add the tenant ID of the main table in the join condition as
		// LEFT JOIN will still return rows that don't satisfy the join condition. We want to add the
		// tenant ID in the where clause instead. The effect of doing a LEFT JOIN WHERE is essentially
		// the same as doing a INNER JOIN in our case. We are doing it because of Postgres performance
		// reasons.
		// Only the leftmost table will be set.
		if node.Jointype == nodes.JOIN_LEFT {
			if n, ok := node.Larg.(nodes.RangeVar); ok {
				if env != nil {
					if n.Alias != nil {
						maybeSetIsLeftJoinTable(env, *n.Alias.Aliasname)
					} else {
						maybeSetIsLeftJoinTable(env, *n.Relname)
					}
				}
			}
		}
		err := t.transformNode(w, node.Larg, env)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (JoinExpr)")
		}
		switch node.Jointype {
		case nodes.JOIN_INNER:
			fmt.Fprint(w, " INNER JOIN ")
		case nodes.JOIN_LEFT:
			fmt.Fprint(w, " LEFT JOIN ")

		default:
			return false, fmt.Errorf("JoinExpr subtype %v not implemented", node.Jointype)

			// case nodes.JOIN_FULL:
			// case nodes.JOIN_RIGHT:
		}
		err = t.transformNode(w, node.Rarg, env)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (JoinExpr)")
		}
		if node.Quals != nil {
			m, err := extractTables([]nodes.Node{node.Larg, node.Rarg})
			if err != nil {
				return false, errors.Wrap(err, "transformNode (JoinExpr)")
			}
			var tables sort.StringSlice
			for table := range m {
				tables = append(tables, table)
			}
			tables.Sort()
			fmt.Fprint(w, " ON ")
			err = t.transformWhereHelper(w, node.Quals, env, false, tables)
			if err != nil {
				return false, errors.Wrap(err, "transformNode (JoinExpr)")
			}
		}
		return false, nil

	case nodes.RangeFunction:
		if len(node.Functions.Items) != 1 {
			return false, fmt.Errorf("%d functions for RangeFunction node, want 1", len(node.Functions.Items))
		}
		f, ok := node.Functions.Items[0].(nodes.List)
		if !ok {
			return false, fmt.Errorf("Functions.Items[0] for RangeFunction node is a %T, want List", node.Functions.Items[0])
		}
		if len(f.Items) == 0 {
			return false, fmt.Errorf("empty subitems list in Functions.Items[0]")
		}
		err := t.transformNode(w, f.Items[0], env)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (RangeFunction)")
		}
		if node.Alias != nil {
			fmt.Fprintf(w, " AS %s(", safestr(*node.Alias.Aliasname))
			err = commaSeparated(w, node.Alias.Colnames.Items, env, func(w io.Writer, col nodes.Node, env environ) error {
				s, ok := col.(nodes.String)
				if !ok {
					return fmt.Errorf("range function alias is a %T, want String", col)
				}
				fmt.Fprint(w, safestr(s.Str))
				return nil
			})
			if err != nil {
				return false, errors.Wrap(err, "transformNode (RangeFunction)")
			}
			fmt.Fprint(w, ")")
		}
		return false, nil

	case nodes.RowExpr:
		fmt.Fprint(w, "(")
		err := commaSeparated(w, node.Args.Items, env, t.transformNode)
		if err != nil {
			return false, errors.Wrap(err, "transformNode (RowExpr)")
		}
		fmt.Fprint(w, ")")
		return true, nil

	case nodes.SQLValueFunction:
		switch node.Op {
		case nodes.SVFOP_CURRENT_TIMESTAMP:
			fmt.Fprint(w, "NOW()")
		default:
			return false, fmt.Errorf("SQLValueFunction %d not handled", node.Op)
		}
		return true, nil

	default:
		return false, fmt.Errorf("node type %T not handled", node)
	}
}

func (t *transformer) specialCaseBoolLiteral(w io.Writer, typecast nodes.TypeCast) bool {
	arg, ok := typecast.Arg.(nodes.A_Const)
	if !ok {
		return false
	}
	s, ok := arg.Val.(nodes.String)
	if !ok {
		return false
	}
	if typecast.TypeName == nil {
		return false
	}
	if len(typecast.TypeName.Names.Items) != 2 {
		return false
	}
	if item, ok := typecast.TypeName.Names.Items[0].(nodes.String); !ok || item.Str != "pg_catalog" {
		return false
	}
	if item, ok := typecast.TypeName.Names.Items[1].(nodes.String); !ok || item.Str != "bool" {
		return false
	}
	switch s.Str {
	case "t":
		fmt.Fprint(w, "true")
	case "f":
		fmt.Fprint(w, "false")
	default:
		return false
	}
	return true
}

func (t *transformer) transformNodeSafe(w io.Writer, node nodes.Node, env environ) error {
	if s, ok := node.(nodes.String); ok {
		fmt.Fprint(w, safestr(s.Str))
		return nil
	}
	return t.transformNode(w, node, env)
}

func (t *transformer) transformIndexElem(w io.Writer, indexElem nodes.IndexElem) error {
	fmt.Fprint(w, safestr(*indexElem.Name))
	return nil
}

func (t *transformer) transformIdent(w io.Writer, names nodes.List) error {
	items := names.Items
	if len(items) == 0 {
		return fmt.Errorf("empty identifier node")
	}
	strItem0, ok := items[0].(nodes.String)
	if !ok {
		return fmt.Errorf("identifier item 0 is a %T, want String", items[0])
	}
	var str string
	switch len(items) {
	case 1:
		str = strItem0.Str

	case 2:
		if strItem0.Str != "pg_catalog" {
			return fmt.Errorf("2-item identifier has \"%s\" prefix, want pg_catalog", strItem0.Str)
		}
		strItem1, ok := items[1].(nodes.String)
		if !ok {
			return fmt.Errorf("identifier item 1 is a %T, want String", items[1])
		}
		str = strings.ToUpper(strItem1.Str)

	default:
		return fmt.Errorf("identifier has %d items, want 1 or 2", len(items))
	}

	switch str {
	case "BOOL":
		str = "BOOLEAN"
	case "INT8":
		str = "BIGINT"
	case "TIMESTAMPTZ":
		str = "TIMESTAMP WITH TIME ZONE"
	}

	fmt.Fprint(w, str)
	return nil
}

func (t *transformer) transformBoolExpr(w io.Writer, boolExpr nodes.BoolExpr, env environ) error {
	switch boolExpr.Boolop {
	case nodes.AND_EXPR:
		for i, arg := range boolExpr.Args.Items {
			if i > 0 {
				fmt.Fprint(w, " AND ")
			}
			err := t.transformBoolSubexpr(w, arg, env)
			if err != nil {
				return errors.Wrap(err, "transformBoolExpr (AND)")
			}
		}

	case nodes.OR_EXPR:
		for i, arg := range boolExpr.Args.Items {
			if i > 0 {
				fmt.Fprint(w, " OR ")
			}
			err := t.transformBoolSubexpr(w, arg, env)
			if err != nil {
				return errors.Wrap(err, "transformBoolExpr (OR)")
			}
		}

	default: // NOT
		fmt.Fprint(w, "NOT ")
		if len(boolExpr.Args.Items) != 1 {
			return fmt.Errorf("NOT expression has %d subexpressions, want 1", len(boolExpr.Args.Items))
		}
		return t.transformBoolSubexpr(w, boolExpr.Args.Items[0], env)
	}

	return nil
}

func (t *transformer) transformBoolSubexpr(w io.Writer, expr nodes.Node, env environ) error {
	if boolExpr, ok := expr.(nodes.BoolExpr); ok {
		fmt.Fprint(w, "(")
		err := t.transformBoolExpr(w, boolExpr, env)
		if err != nil {
			return errors.Wrap(err, "transformBoolSubexpr")
		}
		fmt.Fprint(w, ")")
		return nil
	}
	return t.transformNode(w, expr, env)
}

func (t *transformer) transformSubLink(w io.Writer, subLink nodes.SubLink, env environ) error {
	switch subLink.SubLinkType {
	case nodes.EXISTS_SUBLINK:
		fmt.Fprint(w, "EXISTS (")
		sel, ok := subLink.Subselect.(nodes.SelectStmt)
		if !ok {
			return fmt.Errorf("EXISTS subselect is a %T, want SelectStmt", subLink.Subselect)
		}
		err := t.transformSelect(w, sel, newEnv(), nil)
		if err != nil {
			return errors.Wrap(err, "transformSubLink (EXISTS)")
		}
		fmt.Fprint(w, ")")

	case nodes.ANY_SUBLINK:
		err := t.transformNode(w, subLink.Testexpr, env)
		if err != nil {
			return errors.Wrap(err, "transformSubLink (ANY)")
		}
		fmt.Fprint(w, " IN (")
		sel, ok := subLink.Subselect.(nodes.SelectStmt)
		if !ok {
			return fmt.Errorf("EXISTS subselect is a %T, want SelectStmt", subLink.Subselect)
		}
		err = t.transformSelect(w, sel, newEnv(), nil)
		if err != nil {
			return errors.Wrap(err, "transformSubLink (ANY)")
		}
		fmt.Fprint(w, ")")

	default:
		return fmt.Errorf("SubLink type %v not implemented", subLink.SubLinkType)

		// case nodes.ALL_SUBLINK:
		// case nodes.ROWCOMPARE_SUBLINK:
		// case nodes.EXPR_SUBLINK:
		// case nodes.MULTIEXPR_SUBLINK:
		// case nodes.ARRAY_SUBLINK:
		// case nodes.CTE_SUBLINK:
	}
	return nil
}

func (t *transformer) transformConst(w io.Writer, node nodes.A_Const) error {
	switch val := node.Val.(type) {
	case nodes.Integer:
		fmt.Fprintf(w, "%d", val.Ival)

	case nodes.Float:
		fmt.Fprintf(w, "%s", val.Str)

	case nodes.String:
		fmt.Fprintf(w, "'%s'", escape(val.Str))

	case nodes.Null:
		fmt.Fprint(w, "NULL")
	}
	return nil
}

func escape(s string) string {
	buf := new(bytes.Buffer)
	for _, c := range s {
		if c == '\'' {
			buf.WriteByte('\'')
		}
		buf.WriteRune(c)
	}
	return buf.String()
}

func (t *transformer) transformTypeName(w io.Writer, typeName nodes.TypeName) error {
	err := t.transformIdent(w, typeName.Names)
	if err != nil {
		return errors.Wrap(err, "transformTypeName")
	}
	if len(typeName.ArrayBounds.Items) > 0 {
		if len(typeName.ArrayBounds.Items) > 1 {
			return fmt.Errorf("%d items in TypeName array bounds, want 0 or 1", len(typeName.ArrayBounds.Items))
		}
		ival, ok := typeName.ArrayBounds.Items[0].(nodes.Integer)
		if !ok {
			return fmt.Errorf("TypeName array bounds item is a %T, want Integer", typeName.ArrayBounds.Items[0])
		}
		if ival.Ival != -1 {
			return fmt.Errorf("TypeName array bounds is %d, want -1", ival.Ival)
		}
		fmt.Fprint(w, "[]")
	}
	return nil
}

func (t *transformer) addTenantID(w io.Writer) {
	fmt.Fprintf(w, "$%d", t.tenantIDNum)
	t.isTransformed = true
}

func safestr(s string) string {
	switch s {
	case "position", "timestamp", "type":
		return pq.QuoteIdentifier(s)
	}
	return s
}

func commaSeparated(w io.Writer, nodelist []nodes.Node, env environ, f func(io.Writer, nodes.Node, environ) error) error {
	for i, node := range nodelist {
		if i > 0 {
			fmt.Fprint(w, ", ")
		}
		err := f(w, node, env)
		if err != nil {
			return err
		}
	}
	return nil
}

// maybeSetIsLeftJoinTable sets the status the main table that the query performs LEFT JOIN on to "isLeftJoinTable".
// There can be only one table in env having "isLeftJoinTable" status.
func maybeSetIsLeftJoinTable(env environ, table string) {
	for _, status := range env {
		if status == isLeftJoinTable {
			return
		}
	}
	env[table] = isLeftJoinTable
}

var paramRefType = reflect.TypeOf(nodes.ParamRef{})

// findMaxParam uses reflection to walk the parse tree, ignoring
// everything except ParamRef nodes.
func findMaxParam(item interface{}) int {
	if node, ok := item.(nodes.ParamRef); ok {
		return node.Number
	}
	if val, ok := item.(reflect.Value); ok {
		if val.Type() == paramRefType {
			return findMaxParam(val.Interface())
		}
		switch val.Kind() {
		case reflect.Ptr, reflect.Interface:
			if !val.IsNil() {
				return findMaxParam(val.Elem())
			}

		case reflect.Struct:
			var result int
			for i := 0; i < val.NumField(); i++ {
				subresult := findMaxParam(val.Field(i))
				if i == 0 || subresult > result {
					result = subresult
				}
			}
			return result

		case reflect.Slice, reflect.Array:
			var result int
			for i := 0; i < val.Len(); i++ {
				subresult := findMaxParam(val.Index(i))
				if i == 0 || subresult > result {
					result = subresult
				}
			}
			return result
		}
		return 0
	}
	return findMaxParam(reflect.ValueOf(item))
}
