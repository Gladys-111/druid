package com.alibaba.druid.sql.dialect.clickhouse.ast;

import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.dialect.clickhouse.visitor.CKVisitor;
import com.alibaba.druid.sql.visitor.SQLASTVisitor;

public class ClickhouseColumnTTL extends ClickhouseColumnConstraint{
    private SQLExpr expr;

    public ClickhouseColumnTTL() {
        super();
    }
    public SQLExpr getExpr() {
        return expr;
    }

    public void setExpr(SQLExpr expr) {
        this.expr = expr;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        if (v instanceof CKVisitor) {
            CKVisitor vv = (CKVisitor) v;
            if (vv.visit(this)) {
                acceptChild(vv, expr);
            }
            vv.endVisit(this);
        }
    }

    public ClickhouseColumnTTL clone() {
        ClickhouseColumnTTL clickhouseColumnTTL = (ClickhouseColumnTTL) super.clone();
        clickhouseColumnTTL.setExpr(expr.clone());
        return clickhouseColumnTTL;
    }
}
